package io.dstlr

import java.util.{Properties, UUID}

import edu.stanford.nlp.ie.util.RelationTriple
import edu.stanford.nlp.ling.CoreAnnotations
import edu.stanford.nlp.pipeline.{CoreDocument, CoreEntityMention, StanfordCoreNLP}
import edu.stanford.nlp.simple.Document
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.{Dataset, SparkSession}

import scala.collection.JavaConversions._
import scala.collection.mutable.{ListBuffer, Map => MMap}

/**
 * Extract raw triples from documents on Solr using CoreNLP.
 */
object ExtractTriples {

  // Used for the full NER, KBP, and entity linking
  @transient lazy val nlp = new StanfordCoreNLP(new Properties() {
    {
      setProperty("annotators", "tokenize,ssplit,pos,lemma,parse,ner,coref,kbp,entitylink")
      setProperty("coref.algorithm", "statistical")
      setProperty("threads", "8")
      setProperty("parse.model", "edu/stanford/nlp/models/srparser/englishSR.ser.gz")
    }
  })

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

    // Start time
    val start = System.currentTimeMillis()

    val ds = if (conf.solr()) {
      solr(spark, conf)
    } else {
      text(spark, conf)
    }

    print(s"Processing ${ds.count()} documents")

    val result = ds
      .repartition(conf.partitions())
      .filter(doc => doc.id != null && doc.id.nonEmpty)
      .filter(doc => doc.contents != null && doc.contents.nonEmpty)
      .filter(doc => new Document(doc.contents).sentences().forall(_.tokens().size() <= conf.sentLengthThreshold()))
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
            nlp.annotate(doc)

            // Increment # tokens
            token_acc.add(doc.tokens().size())

            // For eacn sentence...
            doc.sentences().foreach(sentence => {
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
        println(nlp.timingInformation())

        mapped

      })
      .flatMap(x => x)

    // Write to parquet file
    result.write.parquet(conf.output())

    val duration = System.currentTimeMillis() - start
    println(s"Took ${duration}ms @ ${doc_acc.value / (duration / 1000)} doc/s, ${token_acc.value / (duration / 1000)} token/s, and ${triple_acc.value / (duration / 1000)} triple/sec")

    spark.stop()

  }

  def text(spark: SparkSession, conf: Conf): Dataset[DocumentRow] = {

    import spark.implicits._

    // Parse JSON -> map to (id, list of content) -> filter out non-paragraphs -> map to HTML-less strings -> concat paragraphs into document
    //    spark.sparkContext.textFile(conf.input())
    //      .map(ujson.read(_))
    //      .map(json => (json("id").str, json("contents").arr.filter(_ != ujson.Null)))
    //      .map(json => (json._1, json._2.filter(x => x.obj.getOrDefault("type", "").str == "sanitized_html")))
    //      .map(json => (json._1, json._2.filter(x => x.obj.getOrDefault("subtype", "").str == "paragraph")))
    //      .map(json => (json._1, json._2.map(x => Jsoup.parse(x.obj.getOrDefault("content", "").str).text())))
    //      .map(json => (json._1, json._2.mkString(" ")))
    //      .toDF("id", "contents")
    //      .as[DocumentRow]

    // Test data
    spark.sparkContext.parallelize(Seq("Barack Obama was born on August 4th, 1961.", "Apple is based in Cupertino.", "Good Technology is a company based in Sunnyvale.", "Isetan is a company based in Paris.", "The International Arctic Research Center is located in Fairbanks, Alaska."))
      .zipWithIndex()
      .map(_.swap)
      .toDF("id", "contents")
      .as[DocumentRow]

  }

  def solr(spark: SparkSession, conf: Conf): Dataset[DocumentRow] = {

    import spark.implicits._

    val options = collection.mutable.Map(
      "collection" -> conf.solrIndex(),
      "query" -> conf.query(),
      "rows" -> conf.rows(),
      "zkhost" -> conf.solrUri()
    )

    // Are we sampling?
    if (conf.solrSamplePercent.isDefined) {
      options("sample_pct") = conf.solrSamplePercent()
      if (conf.solrSampleSeed.isDefined) {
        options("sample_seed") = conf.solrSampleSeed()
      }
    }

    // Create a DataFrame with the query results
    spark.read.format("solr")
      .options(options)
      .load()
      .as[DocumentRow]

  }

  def toLemmaString(mention: CoreEntityMention): String = {
    mention.tokens()
      .filter(x => !x.tag.matches("[.?,:;'\"!]"))
      .map(token => if (token.lemma() == null) token.word() else token.lemma())
      .mkString(" ")
  }

  def buildMention(doc: String, uuid: String, mention: CoreEntityMention): TripleRow = {

    val meta = MMap(
      "class" -> mention.entityType(),
      "span" -> mention.text(),
      "begin" -> mention.charOffsets().first.toString,
      "end" -> mention.charOffsets().second.toString
    )

    val entityType = mention.entityType()

    // If the entity is annotated by SUTIME, save the normalized time.
    if (entityType == "DATE" || entityType == "DURATION" || entityType == "TIME" || entityType == "SET") {
      meta("normalized") = mention.coreMap().get(classOf[CoreAnnotations.NormalizedNamedEntityTagAnnotation])
    }

    new TripleRow(doc, "Document", doc, "MENTIONS", "Mention", uuid, meta.toMap)
  }

  def buildLinksTo(doc: String, mention: String, uri: String): TripleRow = {
    new TripleRow(doc, "Mention", mention, "LINKS_TO", "Entity", uri, null)
  }

  def buildRelation(doc: String, uuids: MMap[String, UUID], triple: RelationTriple): TripleRow = {
    val sub = uuids.getOrDefault(triple.subjectLemmaGloss(), null).toString
    val rel = triple.relationGloss().replaceAll(":", "_").toUpperCase()
    val obj = uuids.getOrDefault(triple.objectLemmaGloss(), null).toString
    new TripleRow(doc, "Mention", sub, rel, "Mention", obj, Map("confidence" -> triple.confidenceGloss()))
  }
}