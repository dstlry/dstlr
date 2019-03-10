package io.dstlr

import java.util.{Properties, UUID}

import com.lucidworks.spark.rdd.SelectSolrRDD
import edu.stanford.nlp.ie.util.RelationTriple
import edu.stanford.nlp.pipeline.{CoreDocument, StanfordCoreNLP}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.solr.client.solrj.SolrQuery
import org.apache.spark.sql.SparkSession

import scala.collection.JavaConversions._
import scala.collection.mutable.{ListBuffer, Map}

object ExtractTriples {

  def main(args: Array[String]): Unit = {

    // Setup config
    val conf = new Conf(args)
    println(conf.summary)

    val (solrUri, solrIndex, searchField, searchTerm, contentField, output, rows, partitions, nlpThreads) =
      (conf.solrUri(), conf.solrIndex(), conf.searchField(), conf.searchTerm(), conf.contentField(), conf.output(), conf.rows(), conf.partitions(), conf.nlpThreads())

    // Build the SparkSession
    val spark = SparkSession
      .builder()
      .appName("dstlr")
      .master("local[*]")
      .getOrCreate()

    // Get the SparkContext from the SparkSession
    val sc = spark.sparkContext

    // Accumulators for keeping track of # tokens and # triples
    val token_acc = sc.longAccumulator("tokens")
    val triple_acc = sc.longAccumulator("triples")

    FileSystem.get(sc.hadoopConfiguration).delete(new Path(output), true)

    // The SolrQuery to execute
    val query = new SolrQuery(searchField + ":" + searchTerm)

    // Start time
    val start = System.currentTimeMillis()

    val rdd = new SelectSolrRDD(solrUri, solrIndex, sc)
      .query(query)
      .rows(rows)
      .repartition(partitions)
      .mapPartitions(part => {

        val nlp = pipeline()

        part.map(solrDoc => {

          // The triples extracted from the document.
          val triples = new ListBuffer[(String, String, String, String, String, String)]()

          // Maps entity names to UUIDs, consistent within a document.
          val uuids = Map[String, UUID]()

          // The document id
          val id = solrDoc.get("id").toString

          // The CoreDocument
          val doc = new CoreDocument(solrDoc.get(contentField).toString)

          // Annotate the document with our pipeline
          nlp.annotate(doc)

          // Increment # of tokens
          token_acc.add(doc.tokens().size())

          // For each sentence...
          doc.sentences().foreach(sentence => {

            // Extract the "MENTIONS", "HAS_STRING", "IS_A", and "LINKS_TO" relations
            sentence.entityMentions().foreach(mention => {

              // Get or set the UUID
              val uuid = uuids.getOrElseUpdate(mention.text(), UUID.randomUUID()).toString

              triples.append(buildMention(id, uuid))
              triples.append(buildHasString(id, uuid, mention.text()))
              triples.append(buildIs(id, uuid, mention.entityType()))
              // triples.append(buildLinksTo(doc, uuid, mention.entity()))

            })

            // Extract the relations between entities.
            sentence.relations().foreach(relation => {
              if (uuids.contains(relation.subjectGloss()) && uuids.contains(relation.objectGloss())) {
                triples.append(buildRelation(id, uuids, relation))
              }
            })
          })

          // Increment # of triples
          triple_acc.add(triples.size())

          // Return the list of triples
          triples.toList

        })
      })
      .flatMap(x => x)

    val df = spark.createDataFrame(rdd).toDF("doc", "subject.type", "subject.value", "relation", "object.type", "object.value")
    df.show()

    df.write.option("header", "true").csv(output)

    val duration = System.currentTimeMillis() - start
    println(s"Took ${duration}ms @ ${token_acc.value / (duration / 1000)} token/s and ${triple_acc.value / (duration / 1000)} triple/sec")

    spark.stop()

  }

  def buildMention(doc: String, entity: String) = {
    (doc, "Document", doc, "MENTIONS", "Entity", entity)
  }

  def buildHasString(doc: String, entity: String, string: String) = {
    (doc, "Entity", entity, "HAS_STRING", "Label", string)
  }

  def buildIs(doc: String, entity: String, entityType: String) = {
    (doc, "Entity", entity, "IS_A", "EntityType", entityType)
  }

  def buildLinksTo(doc: String, entity: String, uri: String) = {
    (doc, "Entity", entity, "LINKS_TO", "URI", uri)
  }

  def buildRelation(doc: String, uuids: Map[String, UUID], triple: RelationTriple) = {
    val sub = uuids.getOrDefault(triple.subjectGloss(), null).toString
    val rel = triple.relationGloss().split(":")(1).toUpperCase()
    val obj = uuids.getOrDefault(triple.objectGloss(), null).toString
    (doc, "Entity", sub, rel, "Entity", obj)
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