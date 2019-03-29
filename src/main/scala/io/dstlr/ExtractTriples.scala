package io.dstlr

import java.util.{Properties, UUID}

import edu.stanford.nlp.ie.util.RelationTriple
import edu.stanford.nlp.pipeline.{CoreDocument, StanfordCoreNLP}
import edu.stanford.nlp.util.Pair
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.SparkSession

import scala.collection.JavaConversions._
import scala.collection.mutable.{ListBuffer, Map => MMap}

/**
  * Extract raw triples from documents on Solr using CoreNLP.
  */
object ExtractTriples {

  object CoreNLP {

    @transient lazy val nlp = new StanfordCoreNLP(props)

    val threads = (Runtime.getRuntime.availableProcessors / 2).toString

    val props = new Properties()
    props.setProperty("annotators", "tokenize,ssplit,pos,depparse,lemma,ner,coref,kbp,entitylink")
    props.setProperty("ner.applyFineGrained", "false")
    props.setProperty("ner.applyNumericClassifiers", "false")
    props.setProperty("ner.useSUTime", "false")
    props.setProperty("coref.algorithm", "statistical")
    props.setProperty("threads", threads)
    props.setProperty("parse.model", "edu/stanford/nlp/models/srparser/englishSR.ser.gz")

  }

  def main(args: Array[String]): Unit = {

    // Setup config
    val conf = new Conf(args)
    println(conf.summary)

    // Build the SparkSession
    val spark = SparkSession
      .builder()
      .appName("dstlr - ExtractTriples")
      .master("local[*]")
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
      .filter(row => row.contents.split(" ").length <= conf.threshold())
      .mapPartitions(part => {

        // The extracted triples
        val triples = new ListBuffer[TripleRow]()

        // UUIDs for entities consistent within documents
        val uuids = MMap[String, UUID]()

        part.map(row => {

          println(s"Processing ${row.id} on ${Thread.currentThread().getName()}")

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
                val uuid = uuids.getOrElseUpdate(mention.text(), UUID.randomUUID()).toString

                triples.append(buildMention(row.id, uuid, mention.charOffsets()))
                triples.append(buildHasString(row.id, uuid, mention.text()))
                triples.append(buildIs(row.id, uuid, mention.entityType()))
                triples.append(buildLinksTo(row.id, uuid, mention.entity()))

              })

              // Extract the relations between entities.
              sentence.relations().foreach(relation => {
                if (uuids.contains(relation.subjectGloss()) && uuids.contains(relation.objectGloss())) {
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
      })
      .flatMap(x => x)

    // Write to parquet file
    result.write.parquet(conf.output())

    val duration = System.currentTimeMillis() - start
    println(s"Took ${duration}ms @ ${token_acc.value / (duration / 1000)} token/s and ${triple_acc.value / (duration / 1000)} triple/sec")

    spark.stop()

  }

  def buildMention(doc: String, entity: String, offsets: Pair[Integer, Integer]): TripleRow = {
    new TripleRow(doc, "Document", doc, "MENTIONS", "Entity", entity, Map("begin" -> offsets.first.toString, "end" -> offsets.second.toString))
  }

  def buildHasString(doc: String, entity: String, string: String): TripleRow = {
    new TripleRow(doc, "Entity", entity, "HAS_STRING", "Label", string, null)
  }

  def buildIs(doc: String, entity: String, entityType: String): TripleRow = {
    new TripleRow(doc, "Entity", entity, "IS_A", "EntityType", entityType, null)
  }

  def buildLinksTo(doc: String, entity: String, uri: String): TripleRow = {
    new TripleRow(doc, "Entity", entity, "LINKS_TO", "URI", uri, null)
  }

  def buildRelation(doc: String, uuids: MMap[String, UUID], triple: RelationTriple): TripleRow = {
    val sub = uuids.getOrDefault(triple.subjectGloss(), null).toString
    val rel = triple.relationGloss().split(":")(1).toUpperCase()
    val obj = uuids.getOrDefault(triple.objectGloss(), null).toString
    new TripleRow(doc, "Entity", sub, rel, "Entity", obj, null)
  }
}