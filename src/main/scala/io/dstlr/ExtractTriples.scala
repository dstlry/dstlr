package io.dstlr

import java.util.{Properties, UUID}

import com.lucidworks.spark.rdd.SelectSolrRDD
import edu.stanford.nlp.ie.util.RelationTriple
import edu.stanford.nlp.pipeline.{CoreDocument, StanfordCoreNLP}
import org.apache.solr.client.solrj.SolrQuery
import org.apache.spark.SparkContext

import scala.collection.JavaConversions._
import scala.collection.mutable.{ListBuffer, Map}

object ExtractTriples {

  def main(args: Array[String]): Unit = {

    // Setup config
    val conf = new Conf(args)
    println(conf.summary)

    val (solrUri, solrIndex, searchField, searchTerm, contentField, partitions, nlpThreads) =
      (conf.solrUri(), conf.solrIndex(), conf.searchField(), conf.searchTerm(), conf.contentField(), conf.partitions(), conf.nlpThreads())

    val sc = new SparkContext("local[*]", "dstlr")

    // Accumulators for keeping track of # tokens and # triples
    val token_acc = sc.longAccumulator("tokens")
    val triple_acc = sc.longAccumulator("triples")

    // The SolrQuery to execute
    val query = new SolrQuery(searchField + ":" + searchTerm)

    // Start time
    val start = System.currentTimeMillis()

    new SelectSolrRDD(solrUri, solrIndex, sc)
      .query(query)
      .mapPartitions(part => {

        val nlp = pipeline()

        part.map(solrDoc => {

          // The triples extracted from the document.
          val triples = new ListBuffer[(String, String, String, String)]()

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

            // Extract the "MENTIONS", "HAS_STRING", "IS_A", and "LINKS_TO" predicates
            sentence.entityMentions().foreach(mention => {

              // Get or set the UUID
              val uuid = uuids.getOrElseUpdate(mention.text(), UUID.randomUUID()).toString

              triples.append(buildMention(id, uuid))
              triples.append(buildHasString(uuid, mention.text()))
              triples.append(buildIs(uuid, mention.entityType()))
              // triples.append(buildLinksTo(uuid, mention.entity()))

            })

            // Extract the predicates between entities.
            sentence.relations().foreach(relation => {
              if (uuids.contains(relation.subjectGloss()) && uuids.contains(relation.objectGloss())) {
                triples.append(buildPredicate(id, uuids, relation))
              }
            })
          })

          // Increment # of triples
          triple_acc.add(triples.size())

          // Return the list of triples
          triples.toList

        })
      })
      .flatMap(list => {
        val triples = new ListBuffer[String]()
        list.foreach(triple => {
          triples.append(triple.productIterator.mkString(" "))
        })
        triples.toList
      })
      .foreach(println)

    val duration = System.currentTimeMillis() - start
    println(s"Took ${duration}ms @ ${token_acc.value / (duration / 1000)} token/s and ${triple_acc.value / (duration / 1000)} triple/sec")

  }

  def wrapType(value: String, valueType: String) = value + ":" + valueType

  def wrapQuotes(text: String) = "\"" + text + "\""

  def buildMention(doc: String, entity: String) = {
    (wrapType(doc, "Document"), "MENTIONS", wrapType(entity, "Entity"), ".")
  }

  def buildHasString(entity: String, string: String) = {
    (wrapType(entity, "Entity"), "HAS_STRING", wrapType(wrapQuotes(string), "Label"), ".")
  }

  def buildIs(entity: String, entityType: String) = {
    (wrapType(entity, "Entity"), "IS_A", wrapType(entityType, "EntityType"), ".")
  }

  def buildLinksTo(entity: String, uri: String) = {
    (wrapType(entity, "Entity"), "LINKS_TO", wrapType(uri, "URI"), ".")
  }

  def buildPredicate(doc: String, uuids: Map[String, UUID], triple: RelationTriple) = {
    val sub = uuids.getOrDefault(triple.subjectGloss(), null).toString
    val rel = triple.relationGloss().split(":")(1).toUpperCase()
    val obj = uuids.getOrDefault(triple.objectGloss(), null).toString
    ((wrapType(sub, "Entitty"), rel, wrapType(obj, "Entity"), "."))
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