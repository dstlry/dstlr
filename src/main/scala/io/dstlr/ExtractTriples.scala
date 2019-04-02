package io.dstlr

import java.util.{Properties, UUID}

import edu.stanford.nlp.ie.util.RelationTriple
import edu.stanford.nlp.pipeline.{CoreDocument, CoreEntityMention, StanfordCoreNLP}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.SparkSession

import scala.collection.JavaConversions._
import scala.collection.mutable.{ListBuffer, Map => MMap}

/**
  * Extract raw triples from documents on Solr using CoreNLP.
  */
object ExtractTriples {

  object CoreNLP {

    // Used for filtering out documents with sentences too long for the KBPAnnotator
    @transient lazy val ssplit = new StanfordCoreNLP(new Properties() {
      {
        setProperty("annotators", "tokenize,ssplit")
        setProperty("threads", "8")
      }
    })

    // Used for the full NER, KBP, and entity linking
    @transient lazy val nlp = new StanfordCoreNLP(new Properties() {
      {
        setProperty("annotators", "tokenize,ssplit,pos,lemma,parse,ner,coref,kbp,entitylink")
        setProperty("ner.applyFineGrained", "false")
        setProperty("ner.applyNumericClassifiers", "false")
        setProperty("ner.useSUTime", "false")
        setProperty("coref.algorithm", "statistical")
        setProperty("threads", "8")
        setProperty("parse.model", "edu/stanford/nlp/models/srparser/englishSR.ser.gz")
      }
    })
  }

  def main(args: Array[String]): Unit = {

    // Setup config
    val conf = new Conf(args)
    println(conf.summary)

    // Build the SparkSession
    val spark = SparkSession
      .builder()
      .appName("dstlr - ExtractTriples")
      .getOrCreate()

    // Import implicit functions from SparkSession
    import spark.implicits._

    // Accumulators for keeping track of # docs, tokens, and triples
    val doc_acc = spark.sparkContext.longAccumulator("docs")
    val token_acc = spark.sparkContext.longAccumulator("tokens")
    val triple_acc = spark.sparkContext.longAccumulator("triples")

    // Delete old output directory
    FileSystem.get(spark.sparkContext.hadoopConfiguration).delete(new Path(conf.output()), true)

    // Options for spark-solr
    val options = Map(
      "collection" -> conf.solrIndex(),
      "query" -> conf.query(),
      "fields" -> conf.fields(),
      "rows" -> conf.rows(),
      "zkhost" -> conf.solrUri()
    )

    // Start time
    val start = System.currentTimeMillis()

    // Create a DataFrame with the query results
    val ds = spark.read.format("solr")
      .options(options)
      .load()
      .as[SolrRow]

    ds.printSchema()

    val result = ds
      .repartition(conf.partitions())
      .filter(row => row.id != null && row.id.nonEmpty)
      .filter(row => row.contents != null && row.contents.nonEmpty)
      .filter(row => row.contents.split(" ").length <= conf.docLengthThreshold())
      .filter(row => {
        val doc = new CoreDocument(row.contents)
        CoreNLP.ssplit.annotate(doc)
        doc.sentences().forall(sent => sent.tokens().size() <= conf.sentLengthThreshold())
      })
      .mapPartitions(part => {

        // The extracted triples
        val triples = new ListBuffer[TripleRow]()

        // UUIDs for entities consistent within documents
        val uuids = MMap[String, UUID]()

        val mapped = part.map(row => {

          println(s"${System.currentTimeMillis()} - Processing ${row.id} on ${Thread.currentThread().getName()}")

          // The extracted triples
          triples.clear()

          // UUIDs for entities consistent within documents
          uuids.clear()

          // Increment # of docs
          doc_acc.add(1)

          try {

            // Create and annotate the CoreNLP Document
            val doc = new CoreDocument(row.contents)
            CoreNLP.nlp.annotate(doc)

            // Increment # tokens
            token_acc.add(doc.tokens().size())

            // For eacn sentence...
            doc.sentences().foreach(sentence => {

              // Extract "MENTIONS", "HAS_STRING", "IS_A", and "LINKS_TO" relations
              sentence.entityMentions().foreach(mention => {

                // Get or set the UUID
                val uuid = uuids.getOrElseUpdate(toLemmaString(mention), UUID.randomUUID()).toString

                triples.append(buildMention(row.id, uuid, mention))
                triples.append(buildLinksTo(row.id, uuid, mention.entity()))

              })

              // Extract the relations between entities.
              sentence.relations().foreach(relation => {
                if (uuids.contains(relation.subjectLemmaGloss()) && uuids.contains(relation.objectLemmaGloss())) {
                  triples.append(buildRelation(row.id, uuids, relation))
                }
              })
            })

          } catch {
            case e: Exception => println(s"Exception when processing ${row.id} - ${e}")
          }

          // Increment # triples
          triple_acc.add(triples.size())

          triples.toList

        })

        // Log timing info
        println(CoreNLP.nlp.timingInformation())

        mapped

      })
      .flatMap(x => x)

    // Write to parquet file
    result.write.parquet(conf.output())

    val duration = System.currentTimeMillis() - start
    println(s"Took ${duration}ms @ ${doc_acc.value / (duration / 1000)} doc/s, ${token_acc.value / (duration / 1000)} token/s, and ${triple_acc.value / (duration / 1000)} triple/sec")

    spark.stop()

  }

  def toLemmaString(mention: CoreEntityMention): String = {
    mention.tokens()
      .filter(x => !x.tag.matches("[.?,:;'\"!]"))
      .map(token => if (token.lemma() == null) token.word() else token.lemma())
      .mkString(" ")
  }

  def buildMention(doc: String, entity: String, mention: CoreEntityMention): TripleRow = {
    new TripleRow(doc, "Document", doc, "MENTIONS", "Entity", entity, Map(
      "label" -> mention.text(),
      "type" -> mention.entityType(),
      "begin" -> mention.charOffsets().first.toString,
      "end" -> mention.charOffsets().second.toString)
    )
  }

  def buildLinksTo(doc: String, entity: String, uri: String): TripleRow = {
    new TripleRow(doc, "Entity", entity, "LINKS_TO", "URI", uri, null)
  }

  def buildRelation(doc: String, uuids: MMap[String, UUID], triple: RelationTriple): TripleRow = {
    val sub = uuids.getOrDefault(triple.subjectLemmaGloss(), null).toString
    val rel = triple.relationGloss().split(":")(1).toUpperCase()
    val obj = uuids.getOrDefault(triple.objectLemmaGloss(), null).toString
    new TripleRow(doc, "Entity", sub, rel, "Entity", obj, null)
  }
}