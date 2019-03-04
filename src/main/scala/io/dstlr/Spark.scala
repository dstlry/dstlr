package io.dstlr

import java.util.{Properties, UUID}

import com.lucidworks.spark.rdd.SelectSolrRDD
import edu.stanford.nlp.ie.util.RelationTriple
import edu.stanford.nlp.pipeline.{CoreDocument, StanfordCoreNLP}
import org.apache.spark.SparkContext
import org.neo4j.driver.v1.{AuthTokens, GraphDatabase, Statement}

import scala.collection.JavaConversions._
import scala.collection.mutable.Map

object Spark {

  def main(args: Array[String]): Unit = {

    // Setup config
    val conf = new Conf(args)
    println(conf.summary)

    val (uri, user, pass, searchField, searchTerm, contentField) = (conf.neoUri(), conf.neoUsername(), conf.neoPassword(), conf.searchField(), conf.searchTerm(), conf.contentField())

    val sc = new SparkContext("local[*]", "dstlr")

    new SelectSolrRDD("localhost:9983", "core17", sc)
      .rows(10000)
      .query(searchField + ":" + searchTerm)
      .foreachPartition(part => {

        // Connect to Neo4j
        val driver = GraphDatabase.driver(uri, AuthTokens.basic(user, pass))

        // Remove existing nodes in graph
        val session = driver.session()

        // Build the CoreNLP pipeline
        val nlp = pipeline()

        part.foreach(solrDoc => {

          val doc = new CoreDocument(solrDoc.get(contentField).toString)

          // The document ID
          val id = solrDoc.get("id").toString

          // Annotate the document using the CoreNLP pipeline
          nlp.annotate(doc)

          // Maps entity names to UUIDs
          val uuids = Map[String, UUID]()

          // Extract each triple
          doc.sentences().foreach(sent => {

            // Extract "mentions", "has-string", "is-a", and "links-to" predicates for entities
            sent.entityMentions().foreach(mention => {

              // Get or set the UUID
              val uuid = uuids.getOrElseUpdate(mention.text(), UUID.randomUUID()).toString

              session.run(buildMention(id, uuid))
              session.run(buildHasString(uuid, mention.text()))
              session.run(buildIs(uuid, mention.entityType()))
              // session.run(buildLinksTo(mention.text(), mention.entity()))

            })

            // Extract the OpenIE (KBP) triples
            sent.relations().foreach(relation => {
              if (uuids.contains(relation.subjectGloss()) && uuids.contains(relation.objectGloss())) {
                session.run(buildPredicate(id, uuids, relation))
              }
            })
          })
        })

        session.close()
        driver.close()

      })

  }

  def buildMention(doc: String, entity: String): Statement = {
    val params = Map("doc" -> doc, "entity" -> entity)
    new Statement("MERGE (d:Document {id: {doc}}) MERGE (e:Entity {id: {entity}}) MERGE (d)-[r:MENTIONS]->(e) RETURN d, r, e", params)
  }

  def buildHasString(entity: String, string: String): Statement = {
    val params = Map("entity" -> entity, "string" -> string)
    new Statement("MERGE (e:Entity {id: {entity}}) MERGE (l:Label {value: {string}}) MERGE (e)-[r:HAS_STRING]->(l) RETURN e, r, l", params)
  }

  def buildIs(entity: String, entityType: String): Statement = {
    val params = Map("entity" -> entity, "entityType" -> entityType)
    new Statement("MERGE (e:Entity {id: {entity}}) MERGE (t:EntityType {value: {entityType}}) MERGE (e)-[r:IS_A]->(t) RETURN e, r, t", params)
  }

  def buildLinksTo(entity: String, uri: String): Statement = {
    val params = Map("entity" -> entity, "uri" -> uri)
    new Statement("MERGE (e:Entity {id: {entity}}) MERGE (u:URI {id: {uri}}) MERGE (e)-[r:LINKS_TO]->(u) RETURN e, r, u", params)
  }

  def buildPredicate(doc: String, uuids: Map[String, UUID], triple: RelationTriple): Statement = {
    val rel = triple.relationGloss().split(":")(1).toUpperCase()
    val params = Map(
      "doc" -> doc,
      "sub" -> uuids.getOrDefault(triple.subjectGloss(), null).toString,
      "obj" -> uuids.getOrDefault(triple.objectGloss(), null).toString
    )
    new Statement(
      s"""
         |MATCH (s:Entity {id:{sub}}),(o:Entity {id:{obj}})
         |MERGE (s)-[r:${rel}]->(o)
         |ON CREATE SET r.docs = [{doc}]
         |ON MATCH SET r.docs = r.docs + [{doc}]
       """.stripMargin, params)
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