package io.dstlr

import java.util.{Properties, UUID}

import edu.stanford.nlp.ie.util.RelationTriple
import edu.stanford.nlp.pipeline.{CoreDocument, StanfordCoreNLP}

import scala.collection.JavaConversions._
import scala.collection.mutable.{ListBuffer, Map}

object Main {

  def main(args: Array[String]): Unit = {

    // Connect to Neo4j
    // val driver = GraphDatabase.driver("bolt://localhost:7687", AuthTokens.basic("neo4j", "password"))

    // Properties for CoreNLP
    val props = new Properties()
    props.setProperty("annotators", "tokenize,ssplit,pos,depparse,lemma,ner,coref,kbp")
//    props.setProperty("annotators", "tokenize,ssplit,pos,depparse,lemma,ner,coref,kbp,entitylink")
    props.setProperty("ner.applyFineGrained", "false")
    props.setProperty("ner.applyNumericClassifiers", "false")
    props.setProperty("ner.useSUTime", "false")

    // Build the CoreNLP pipeline
    val pipeline = new StanfordCoreNLP(props)

    // Create some CoreDocuments
    val docs = new ListBuffer[CoreDocument]()
    docs += new CoreDocument("Barack Obama lives in Hawaii. He was born in 1961.")
    docs += new CoreDocument("Apple is a company based in Cupertino.")
    docs += new CoreDocument("Bill Clinton is married to Hillary Clinton.")

    // For each doc...
    docs.foreach(doc => {

      // The document ID
      val id = docs.indexOf(doc)

      // Annotate the document using the CoreNLP pipeline
      pipeline.annotate(doc)

      // Maps entity names to UUIDs
      val uuids = Map[String, UUID]()

      // Extract each triple
      doc.sentences().foreach(sent => {

        // Extract "mentions" and "is-a" triples
        sent.entityMentions().foreach(mention => {

          // Get or set the UUID
          val uuid = uuids.getOrElseUpdate(mention.text(), UUID.randomUUID())

          println(s"(${id}, mentions, ${uuid})")
          println(s"(${uuid}, has-string, ${mention.text()})")
          println(s"(${uuid}, is-a, ${mention.entityType()})")
//          println(s"(${mention.text()}, links-to, ${mention.entity()})")

          sent.entityMentions()

        })

        sent.relations().foreach(triple => {
          println(makeTriple(triple))
        })

        // val session = driver.session()
        // session.run(s"CREATE (s:Subject {value:'${triple._1}'}) -[rel:${triple._2.replaceAll("\\s+", "_")}]-> (o:Object {value:'${triple._3}'}) RETURN s, o")
        // session.close()

      })

      println()

    })

    // driver.close()

  }

  def makeTriple(triple: RelationTriple): (String, String, String) = {
    (triple.subjectLemmaGloss(), triple.relationLemmaGloss(), triple.objectLemmaGloss())
  }

}