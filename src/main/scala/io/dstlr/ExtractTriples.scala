package io.dstlr

import java.util.{Properties, UUID}

import edu.stanford.nlp.ie.util.RelationTriple
import edu.stanford.nlp.pipeline.{CoreDocument, StanfordCoreNLP}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.SparkSession

import scala.collection.JavaConversions._
import scala.collection.mutable.{ListBuffer, Map}

object ExtractTriples {

  // Result from spark-solr
  case class SolrRow(id: String, raw: String)

  // Result of our extraction
  case class TripleRow(doc: String, subjectType: String, subjectValue: String, relation: String, objectType: String, objectValue: String)

  def main(args: Array[String]): Unit = {

    // Setup config
    val conf = new Conf(args)
    println(conf.summary)

    // Build the SparkSession
    val spark = SparkSession
      .builder()
      .appName("dstlr")
      .master("local[*]")
      .getOrCreate()

    // Import implicit functions from SparkSession
    import spark.implicits._

    // Accumulators for keeping track of # tokens and # triples
    val token_acc = spark.sparkContext.longAccumulator("tokens")
    val triple_acc = spark.sparkContext.longAccumulator("triples")

    // Delete old output directory
    FileSystem.get(spark.sparkContext.hadoopConfiguration).delete(new Path(conf.output()), true)

    // Options for spark-solr
    val options = Map(
      "collection" -> conf.solrIndex(),
      "query" -> conf.query(),
      "rows" -> conf.rows(),
      "zkhost" -> conf.solrUri()
    )

    // Start time
    val start = System.currentTimeMillis()

    // Create a DataFrame with the query results
    val df = spark.read.format("solr")
      .options(options)
      .load()

    // Get as a DataSet
    val ds = df.as[SolrRow]
    ds.printSchema()

    ds
      .repartition(conf.partitions())
      .mapPartitions(part => {

        // Create CoreNLP pipeline
        val nlp = pipeline()

        part.map(row => {

          println(s"Processing ${row.id}")

          // The extracted triples
          val triples = new ListBuffer[TripleRow]()

          // UUIDs for entities consistent within documents
          val uuids = Map[String, UUID]()

          // Create and annotate the CoreNLP Document
          val doc = new CoreDocument(row.raw)
          nlp.annotate(doc)

          // Increment # tokens
          token_acc.add(doc.tokens().size())

          // For eacn sentence...
          doc.sentences().foreach(sentence => {

            // Extract "MENTIONS", "HAS_STRING", "IS_A", and "LINKS_TO" relations
            sentence.entityMentions().foreach(mention => {

              // Get or set the UUID
              val uuid = uuids.getOrElseUpdate(mention.text(), UUID.randomUUID()).toString

              triples.append(buildMention(row.id, uuid))
              triples.append(buildHasString(row.id, uuid, mention.text()))
              triples.append(buildIs(row.id, uuid, mention.entityType()))
              // triples.append(buildLinksTo(row.id, uuid, mention.entity()))

            })

            // Extract the relations between entities.
            sentence.relations().foreach(relation => {
              if (uuids.contains(relation.subjectGloss()) && uuids.contains(relation.objectGloss())) {
                triples.append(buildRelation(row.id, uuids, relation))
              }
            })
          })

          // Increment # triples
          triple_acc.add(triples.size())

          triples.toList

        })
      })
      .flatMap(x => x)
      .write.option("header", "true").csv(conf.output())

    val duration = System.currentTimeMillis() - start
    println(s"Took ${duration}ms @ ${token_acc.value / (duration / 1000)} token/s and ${triple_acc.value / (duration / 1000)} triple/sec")

    spark.stop()

  }

  def buildMention(doc: String, entity: String): TripleRow = {
    new TripleRow(doc, "Document", doc, "MENTIONS", "Entity", entity)
  }

  def buildHasString(doc: String, entity: String, string: String): TripleRow = {
    new TripleRow(doc, "Entity", entity, "HAS_STRING", "Label", string)
  }

  def buildIs(doc: String, entity: String, entityType: String): TripleRow = {
    new TripleRow(doc, "Entity", entity, "IS_A", "EntityType", entityType)
  }

  def buildLinksTo(doc: String, entity: String, uri: String): TripleRow = {
    new TripleRow(doc, "Entity", entity, "LINKS_TO", "URI", uri)
  }

  def buildRelation(doc: String, uuids: Map[String, UUID], triple: RelationTriple): TripleRow = {
    val sub = uuids.getOrDefault(triple.subjectGloss(), null).toString
    val rel = triple.relationGloss().split(":")(1).toUpperCase()
    val obj = uuids.getOrDefault(triple.objectGloss(), null).toString
    new TripleRow(doc, "Entity", sub, rel, "Entity", obj)
  }

  def pipeline(): StanfordCoreNLP = {

    // Properties for CoreNLP
    val props = new Properties()
    props.setProperty("annotators", "tokenize,ssplit,pos,depparse,lemma,ner,coref,kbp") // entitylink
    props.setProperty("ner.applyFineGrained", "false")
    props.setProperty("ner.applyNumericClassifiers", "false")
    props.setProperty("ner.useSUTime", "false")
    props.setProperty("coref.algorithm", "statistical")

    // Build the CoreNLP pipeline
    new StanfordCoreNLP(props)
  }
}