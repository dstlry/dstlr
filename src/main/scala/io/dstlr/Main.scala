package io.dstlr

import java.util.{Properties, UUID}

import edu.stanford.nlp.ie.util.RelationTriple
import edu.stanford.nlp.pipeline.{CoreDocument, StanfordCoreNLP}
import org.neo4j.driver.v1.{AuthTokens, GraphDatabase}
import org.rogach.scallop.ScallopConf

import scala.collection.JavaConversions._
import scala.collection.mutable.{ListBuffer, Map}

object Main {

  class Conf(args: Seq[String]) extends ScallopConf(args) {

    val neoUri = opt[String](default = Some("bolt://localhost:7687"))
    val neoUsername = opt[String](default = Some("neo4j"))
    val neoPassword = opt[String](name = "neo4j.password", default = Some("neo4j"))

    verify()

  }

  def main(args: Array[String]): Unit = {

    // Setup config
    val conf = new Conf(args)
    println(conf.summary)

    // Connect to Neo4j
    val driver = GraphDatabase.driver(conf.neoUri(), AuthTokens.basic(conf.neoUsername(), conf.neoPassword()))
    val session = driver.session()
    session.run("MATCH (n) DETACH DELETE n")

    // Properties for CoreNLP
    val props = new Properties()
    props.setProperty("annotators", "tokenize,ssplit,pos,depparse,lemma,ner,coref,kbp") // entitylink
    props.setProperty("ner.applyFineGrained", "false")
    props.setProperty("ner.applyNumericClassifiers", "false")
    props.setProperty("ner.useSUTime", "false")

    // Build the CoreNLP pipeline
    val pipeline = new StanfordCoreNLP(props)

    // Create some CoreDocuments
    val docs = new ListBuffer[CoreDocument]()
    docs += new CoreDocument("Barack Obama was born in 1961.")
    docs += new CoreDocument("Barack Obama was born in 1961.")
    //    docs += new CoreDocument("Barack Obama lives in Hawaii. He was born in 1961.")
    //    docs += new CoreDocument("Apple is a company based in Cupertino.")
    //    docs += new CoreDocument("Bill Clinton is married to Hillary Clinton.")

    // Maps entity names to UUIDs
    val uuids = Map[String, UUID]()

    // For each doc...
    docs.foreach(doc => {

      // The document ID
      val id = docs.indexOf(doc).toString

      // Annotate the document using the CoreNLP pipeline
      pipeline.annotate(doc)

      // Extract each triple
      doc.sentences().foreach(sent => {

        // Extract "mentions", "has-string", and "is-a" predicates for entities
        sent.entityMentions().foreach(mention => {

          // Get or set the UUID
          val uuid = uuids.getOrElseUpdate(mention.text(), UUID.randomUUID())

          insertMention(id, uuid.toString)
          insertHasString(uuid.toString, mention.text())
          insertIsA(uuid.toString, mention.entityType())
          // insertLinksTo(mention.text(), mention.entity())

        })

        sent.relations().foreach(relation => {
          insertTriple(id, relation)
        })

      })

    })

    session.close()
    driver.close()

    def insertMention(doc: String, entity: String) = {
      session.run(s"MERGE (d:Document {id: '${doc}'}) MERGE (e:Entity {id: '${entity}'}) MERGE (d)-[r:mentions]->(e) RETURN d, r, e")
    }

    def insertHasString(entity: String, string: String) = {
      session.run(s"MERGE (e:Entity {id:'${entity}'}) MERGE (l:Label {value:'${string}'}) MERGE (e)-[r:has_string]->(l) RETURN e, r, l")
    }

    def insertIsA(entity: String, entityType: String) = {
      session.run(s"MERGE (e:Entity {id:'${entity}'}) MERGE (t:EntityType {value:'${entityType}'}) MERGE (e)-[r:is_a]->(t) RETURN e, r, t")
    }

    def insertLinksTo(entity: String, uri: String) = {
      session.run(s"MERGE (e:Entity {id:'${entity}'}) MERGE (u:URI {id:'${uri}'}) MERGE (e)-[r:links_to]->(u) RETURN e, r, u")
    }

    def insertTriple(doc: String, triple: RelationTriple) = {
      val sub = uuids.getOrDefault(triple.subjectGloss(), null).toString
      val rel = triple.relationGloss().split(":")(1)
      val obj = uuids.getOrDefault(triple.objectGloss(), null).toString
      session.run(s"MERGE (s:Entity {id:'${sub}'}) MERGE (o:Entity {id:'${obj}'}) CREATE (s)-[r:${rel} {doc:[${doc}]}]->(o) RETURN s, r, o")
    }
  }
}