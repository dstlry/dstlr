package io.dstlr

import java.text.SimpleDateFormat

import com.softwaremill.sttp._
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.{Row, SparkSession}
import ujson.Value

import scala.collection.mutable.{ListBuffer, Map => MMap}

/**
  * Enrich the "LINKS_TO" relationships of our extracted triples using data from WikiData.
  */
object EnrichTriples {

  val dateFormat = new SimpleDateFormat("'+'yyyy-MM-dd'T'HH:mm:ss'Z'")
  val printFormat = new SimpleDateFormat("yyyy-MM-dd")

  def main(args: Array[String]): Unit = {

    val conf = new Conf(args)
    println(conf.summary)

    val spark = SparkSession
      .builder()
      .appName("dstlr - EnrichTriples")
      .getOrCreate()

    import spark.implicits._

    val mapping = spark.sparkContext.broadcast(
      spark.read.option("header", "true").csv("wikidata.csv").as[KnowledgeGraphMappingRow].rdd.map(row => (row.property, row.relation)).collectAsMap()
    )

    // Delete old output directory
    FileSystem.get(spark.sparkContext.hadoopConfiguration).delete(new Path(conf.output()), true)

    val entities = spark.read.parquet(conf.input()).as[TripleRow]
      .repartition(conf.partitions())
      .filter($"relation" === "LINKS_TO" && $"objectValue".isNotNull)
      .select($"objectValue")
      .distinct()

    val result = entities
      .mapPartitions(part => {

        // Standard HTTP backend
        implicit val backend = HttpURLConnectionBackend()

        part.grouped(50).map(group => {

          // Holds extracted triples
          val list = ListBuffer[TripleRow]()

          try {

            val id2title = mapTitles(group)

            val ids = id2title.keys.mkString("|")

            val resp = sttp.get(uri"https://www.wikidata.org/w/api.php?action=wbgetentities&ids=${ids}&languages=en&format=json").send()
            val json = ujson.read(resp.unsafeBody)

            val entities = json("entities")

            entities.obj
              .filter(entity => id2title.contains(entity._1))
              .foreach(entity => {

                val (id, content) = entity

                val title = id2title.get(id).get
                val claims = content("claims")

                println(s"###\n# ${id} -> ${title}\n###")

                mapping.value.foreach(map => {
                  val (property, relation) = map
                  if (claims.obj.contains(property)) {
                    println(s"${property} -> ${relation}")
                    try {
                      relation match {
                        case "CITY_OF_HEADQUARTERS" => list.append(extractHeadquarters(title, relation, claims(property)))
                        case _ => // DUMMY
                      }
                    } catch {
                      case t: Throwable => println(s"Error processing ${id} - ${t}")
                    }
                  }
                })
              })

          } catch {
            case e: Exception => {
              println(s"Error processing group (${group})")
              println(e)
            }
          }

          // Return triples
          list

        })
      })
      .flatMap(x => x)

    result.write.parquet(conf.output())

    spark.stop()

  }

  def mapTitles(group: Seq[Row]): Map[String, String] = {

    // Standard HTTP backend
    implicit val backend = HttpURLConnectionBackend()

    // Wiki APIs takes "|" delimited titles
    val titles = group.map(_.getString(0)).mkString("|")

    val resp = sttp.get(uri"https://en.wikipedia.org/w/api.php?action=query&prop=pageprops&ppprop=wikibase_item&redirects=1&format=json&titles=${titles}").send()
    val json = ujson.read(resp.unsafeBody)

    val normalized = MMap[String, String]()
    val redirects = MMap[String, String]()

    // Mapping back from normalized title to original title
    json("query")("normalized").arr.foreach(element => normalized(element("to").str) = element("from").str)

    // If the re-directs are present, add to the Map.
    if (json("query").obj.contains("redirects")) {
      json("query")("redirects").arr.foreach(element => normalized(element("to").str) = element("from").str)
    }

    json("query")("pages").obj
      .filter(row => {
        // Items without WikiBase entities are returned with IDs -1, -2, -3...
        !row._1.startsWith("-")
      })
      .filter(_._2.obj.contains("pageprops"))
      .filter(_._2.obj("pageprops").obj.contains("wikibase_item"))
      .map(row => {
        val mappedTitle = row._2("title").str
        val redirectedTitle = redirects.getOrElse(mappedTitle, mappedTitle)
        val originalTitle = normalized.getOrElse(redirectedTitle, redirectedTitle)
        val wikiBaseId = row._2("pageprops")("wikibase_item").str
        (wikiBaseId, originalTitle)
      })
      .toMap
  }

  def extractBirthDate(uri: String, relation: String, json: Value): TripleRow = {
    val date = dateFormat.parse(json(0)("mainsnak")("datavalue")("value")("time").str)
    new TripleRow("wiki", "Entity", uri, relation, "Fact", printFormat.format(date), null)
  }

  def extractDeathDate(uri: String, relation: String, json: Value): TripleRow = {
    val date = dateFormat.parse(json(0)("mainsnak")("datavalue")("value")("time").str)
    new TripleRow("wiki", "Entity", uri, relation, "Fact", printFormat.format(date), null)
  }

  def extractHeadquarters(uri: String, relation: String, json: Value): TripleRow = {
    val wid = json(0)("mainsnak")("datavalue")("value")("id").str
    new TripleRow("wiki", "Entity", uri, relation, "Fact", id2label(wid), null)
  }

  def id2label(id: String): String = {
    implicit val backend = HttpURLConnectionBackend()
    val resp = sttp.get(uri"https://www.wikidata.org/w/api.php?action=wbgetentities&props=labels&ids=${id}&languages=en&format=json").send()
    val json = ujson.read(resp.unsafeBody)
    json("entities")(id)("labels")("en")("value").str
  }
}