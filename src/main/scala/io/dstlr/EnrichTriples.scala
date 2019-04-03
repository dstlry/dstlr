package io.dstlr

import java.text.SimpleDateFormat

import com.softwaremill.sttp._
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.{Row, SparkSession}
import ujson.Value

import scala.collection.mutable.ListBuffer

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
      spark.read.option("header", "true").csv("wikidata.csv").as[WikiDataMappingRow].rdd.map(row => (row.property, row.relation)).collectAsMap()
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

          val id2title = mapTitles(group)

          val ids = id2title.keys.mkString("|")

          val resp = sttp.get(uri"https://www.wikidata.org/w/api.php?action=wbgetentities&ids=${ids}&languages=en&format=json").send()
          val json = ujson.read(resp.unsafeBody)

          val entities = json("entities")

          entities.obj.foreach(entity => {

            val (id, content) = entity

            val title = id2title.get(id).get
            val claims = content("claims")

            println(s"###\n# ${id} -> ${title}\n###")

            mapping.value.foreach(map => {
              val (property, relation) = map
              if (claims.obj.contains(property)) {
                println(s"${property} -> ${relation}")
                relation match {
                  case "CITY_OF_HEADQUARTERS" => list.append(extractHeadquarters(title, relation, claims(property)))
                  case _ => // DUMMY
                }
              }
            })
          })

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

    // Mapping back from normalized title to original title
    val normalized = json("query")("normalized").arr
      .map(element => (element("to").str, element("from").str))
      .toMap

    // Mapping back from re-direct to original
    val redirects = json("query")("redirects").arr
      .map(element => (element("to").str, element("from").str))
      .toMap

    json("query")("pages").obj
      .filter(row => {
        // Items without WikiBase entities are returned with IDs -1, -2, -3...
        !row._1.startsWith("-")
      })
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
    new TripleRow("wiki", "URI", uri, relation, "WikiDataValue", printFormat.format(date), null)
  }

  def extractDeathDate(uri: String, relation: String, json: Value): TripleRow = {
    val date = dateFormat.parse(json(0)("mainsnak")("datavalue")("value")("time").str)
    new TripleRow("wiki", "URI", uri, relation, "WikiDataValue", printFormat.format(date), null)
  }

  def extractHeadquarters(uri: String, relation: String, json: Value): TripleRow = {
    val wid = json(0)("mainsnak")("datavalue")("value")("id").str
    new TripleRow("wiki", "URI", uri, relation, "WikiDataValue", id2label(wid), null)
  }

  // Go from WikiData ID to Label
  def id2label(id: String): String = {
    implicit val backend = HttpURLConnectionBackend()
    val resp = sttp.get(uri"https://www.wikidata.org/w/api.php?action=wbgetentities&props=labels&ids=${id}&languages=en&format=json").send()
    val json = ujson.read(resp.unsafeBody)
    json("entities")(id)("labels")("en")("value").str
  }
}