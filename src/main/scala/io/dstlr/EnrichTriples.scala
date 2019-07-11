package io.dstlr

import java.text.SimpleDateFormat
import java.util.function.Consumer

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.jena.query
import org.apache.jena.query.QuerySolution
import org.apache.jena.rdfconnection.{RDFConnection, RDFConnectionFuseki}
import org.apache.spark.sql.SparkSession

import scala.collection.mutable.ListBuffer

/**
  * Enrich the "LINKS_TO" relationships of our extracted triples using data from WikiData.
  */
object EnrichTriples {

  val dateFormat = new SimpleDateFormat("yyyy-MM-dd")
  val jenaFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'")

  def main(args: Array[String]): Unit = {

    // Command line args
    val conf = new Conf(args)
    println(conf.summary)

    // Initialize Spark
    val spark = SparkSession
      .builder()
      .appName("dstlr - EnrichTriples")
      .getOrCreate()

    import spark.implicits._

    // Delete old output directory
    FileSystem.get(spark.sparkContext.hadoopConfiguration).delete(new Path(conf.output()), true)

    // Mapping from Wikidata Property ID to CoreNLP relation name
    val property2relation = spark.sparkContext.broadcast(
      spark.read.option("header", "true").csv("wikidata.csv").as[KnowledgeGraphMappingRow].rdd.map(row => (row.property, row.relation)).filter(row => row._1 != null && row._2 != null).collectAsMap()
    )

    // The distinct entities extracted from documents
    val entities = spark.read.parquet(conf.input()).as[TripleRow]
      .filter($"relation" === "LINKS_TO" && $"objectValue".isNotNull)
      .select($"objectValue")
      .distinct()
      .repartition(conf.partitions())

    val result = entities
      .map(row => (row.getString(0), getWikidataId(conf.jenaUri(), row.getString(0))))
      .filter(row => row._2 != null)
      .map(row => {

        val list = new ListBuffer[TripleRow]()

        val (name, id) = row
        val properties = getProperties(conf.jenaUri(), s"<${id}>")

        properties.foreach(property => {
          try {
            property match {
              case "P159" => list.append(extractCityOfHeadquarters(conf.jenaUri(), name, id, property2relation.value(property), property))
              case "P569" => list.append(extractDateOfBirth(conf.jenaUri(), name, id, property2relation.value(property), property))
              case "P570" => list.append(extractDateOfDeath(conf.jenaUri(), name, id, property2relation.value(property), property))
              case _ => // DUMMY
            }
          } catch {
            case e: Exception => println(s"Error processing ${property} for ${name} (${id}): ${e}")
          }
        })

        list

      })
      .flatMap(x => x)

    result.write.parquet(conf.output())

  }

  def getWikidataId(jenaUri: String, entity: String): String = {

    var id: String = null
    var connection: RDFConnection = null

    val encodedEntity = s"<https://en.wikipedia.org/wiki/${encodeEntity(entity)}>"

    try {
      connection = RDFConnectionFuseki.create().destination(jenaUri).build()
      connection.querySelect(s"SELECT ?object WHERE { ${encodedEntity} <http://schema.org/about> ?object }", new Consumer[QuerySolution] {
        override def accept(t: QuerySolution): Unit = {
          id = t.getResource("object").getURI()
        }
      })
    } finally {
      if (connection != null) {
        connection.close()
      }
    }

    id

  }

  def getProperties(jenaUri: String, entity: String): List[String] = {

    val result = new ListBuffer[String]()
    var connection: RDFConnection = null

    try {
      connection = RDFConnectionFuseki.create().destination(jenaUri).build()
      connection.queryResultSet(s"SELECT DISTINCT ?predicate WHERE { ${entity} ?predicate ?object . FILTER regex(str(?predicate), 'http://www.wikidata.org/prop/direct/P[0-9]+')}", new Consumer[query.ResultSet] {
        override def accept(t: query.ResultSet): Unit = {
          while (t.hasNext()) {
            val uri = t.next().getResource("predicate").getURI()
            result.append(uri.substring(uri.lastIndexOf("/") + 1, uri.length))
          }
        }
      })
    } finally {
      if (connection != null) {
        connection.close()
      }
    }

    result.toList

  }

  def getProperty(jenaUri: String, entity: String, propertyId: String, extractor: QuerySolution => Any): Any = {

    var result: Any = null
    var connection: RDFConnection = null

    try {
      connection = RDFConnectionFuseki.create().destination(jenaUri).build()
      connection.querySelect(s"SELECT * WHERE { <${entity}> <http://www.wikidata.org/prop/direct/${propertyId}> ?object . OPTIONAL { ?object <http://schema.org/name> ?name . }}", new Consumer[QuerySolution] {
        override def accept(qs: QuerySolution): Unit = (result = extractor(qs))
      })
    } finally {
      if (connection != null) {
        connection.close()
      }
    }

    result

  }

  def encodeEntity(entity: String): String = {
    entity
      .replaceAll("\"", "%22")
      .replaceAll("`", "%60")
  }

  def extractCityOfHeadquarters(jenaUri: String, name: String, id: String, relation: String, property: String): TripleRow = {
    val fact = getProperty(jenaUri, id, property, x => if (x.contains("name")) x.getLiteral("name").getString else null).asInstanceOf[String]
    new TripleRow("wiki", "Entity", name, relation, "Fact", fact, null)
  }

  def extractDateOfBirth(jenaUri: String, name: String, id: String, relation: String, property: String): TripleRow = {
    val dateStr = getProperty(jenaUri, id, property, x => if (x.contains("object")) x.getLiteral("object").getString else null).asInstanceOf[String]
    new TripleRow("wiki", "Entity", name, relation, "Fact", dateFormat.format(jenaFormat.parse(dateStr)), null)
  }

  def extractDateOfDeath(jenaUri: String, name: String, id: String, relation: String, property: String): TripleRow = {
    val dateStr = getProperty(jenaUri, id, property, x => if (x.contains("object")) x.getLiteral("object").getString else null).asInstanceOf[String]
    new TripleRow("wiki", "Entity", name, relation, "Fact", dateFormat.format(jenaFormat.parse(dateStr)), null)
  }
}