package io.dstlr

import java.util.{Properties, UUID}

import edu.stanford.nlp.ie.util.RelationTriple
import edu.stanford.nlp.pipeline.{CoreDocument, StanfordCoreNLP}
import org.neo4j.driver.v1.{AuthTokens, GraphDatabase, Statement}

import scala.collection.JavaConversions._
import scala.collection.mutable.{ListBuffer, Map}

object Main {

  def main(args: Array[String]): Unit = {

    // Setup config
    val conf = new Conf(args)
    println(conf.summary)

    // Connect to Neo4j
    val driver = GraphDatabase.driver(conf.neoUri(), AuthTokens.basic(conf.neoUsername(), conf.neoPassword()))

    // Remove existing nodes in graph
    val session = driver.session()
    session.run("MATCH (n) DETACH DELETE n")

    // Build the CoreNLP pipeline
    val nlp = pipeline()

    // Create some CoreDocuments
    val docs = generateDocs()

    // For each doc...
    docs.foreach(doc => {

      // The document ID
      val id = docs.indexOf(doc).toString

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
          session.run(buildPredicate(id, uuids, relation))
        })
      })
    })

    session.close()
    driver.close()

  }

  def buildMention(doc: String, entity: String): Statement = {
    new Statement(s"MERGE (d:Document {id: '${doc}'}) MERGE (e:Entity {id: '${entity}'}) MERGE (d)-[r:MENTIONS]->(e) RETURN d, r, e")
  }

  def buildHasString(entity: String, string: String): Statement = {
    new Statement(s"MERGE (e:Entity {id: '${entity}'}) MERGE (l:Label {value: '${string}'}) MERGE (e)-[r:HAS_STRING]->(l) RETURN e, r, l")
  }

  def buildIs(entity: String, entityType: String): Statement = {
    new Statement(s"MERGE (e:Entity {id: '${entity}'}) MERGE (t:EntityType {value: '${entityType}'}) MERGE (e)-[r:IS_A]->(t) RETURN e, r, t")
  }

  def buildLinksTo(entity: String, uri: String): Statement = {
    new Statement(s"MERGE (e:Entity {id: '${entity}'}) MERGE (u:URI {id: '${uri}'}) MERGE (e)-[r:LINKS_TO]->(u) RETURN e, r, u")
  }

  def buildPredicate(doc: String, uuids: Map[String, UUID], triple: RelationTriple) = {
    val sub = uuids.getOrDefault(triple.subjectGloss(), null).toString
    val rel = triple.relationGloss().split(":")(1).toUpperCase()
    val obj = uuids.getOrDefault(triple.objectGloss(), null).toString
    new Statement(s"""
         |MATCH (s:Entity {id:'${sub}'}),(o:Entity {id:'${obj}'})
         |MERGE (s)-[r:${rel}]->(o)
         |ON CREATE SET r.docs = ['${doc}']
         |ON MATCH SET r.docs = r.docs + ['${doc}']
       """.stripMargin)
  }

  def generateDocs(): List[CoreDocument] = {
    val docs = new ListBuffer[CoreDocument]()
    docs += new CoreDocument("Barack Obama is from Hawaii. He was born in 1961.")
    docs += new CoreDocument("Apple is a company based in Cupertino.")
    docs += new CoreDocument("Bill Clinton is married to Hillary Clinton.")
    docs.toList
  }

  def pipeline(): StanfordCoreNLP = {

    // Properties for CoreNLP
    val props = new Properties()
    props.setProperty("annotators", "tokenize,ssplit,pos,depparse,lemma,ner,coref,kbp") // entitylink
    props.setProperty("ner.applyFineGrained", "false")
    props.setProperty("ner.applyNumericClassifiers", "false")
    props.setProperty("ner.useSUTime", "false")

    // Build the CoreNLP pipeline
    new StanfordCoreNLP(props)
  }
}