package io.dstlr

import java.text.SimpleDateFormat

import com.softwaremill.sttp._
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.SparkSession
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

        val list = new ListBuffer[TripleRow]()

        // WikiData API can take 50 titles at a time
        part.grouped(50).map(batch => {

          // WikiData API takes "|" delimited titles
          val titles = batch.map(_.getString(0)).mkString("|")

          // Send the request
          val resp = sttp.get(uri"https://www.wikidata.org/w/api.php?action=wbgetentities&sites=enwiki&titles=${titles}&languages=en&format=json").send()

          // Parse JSON response
          val json = ujson.read(resp.unsafeBody)

          val entities = json("entities")

          for ((ent, idx) <- entities.obj.zipWithIndex) {

            // The WikiData ID
            val id = ent._1

            // The WikiData title
            val title = batch(idx).getString(0)

            println(s"###\n# ${id} -> ${title}\n###")
            val claims = ent._2("claims")

            mapping.value.foreach(map => {

              val (propertyId, relation) = map

              // If the entity has one of our properties...
              if (claims.obj.contains(propertyId)) {

                println(s"${propertyId} -> ${relation}")

                // Match over relation names (DATE_OF_BIRTH, DATE_OF_DEATH, etc.)
                map._2 match {
                  case "DATE_OF_BIRTH" => list.append(extractBirthDate(title, relation, claims(propertyId)))
                  case "DATE_OF_DEATH" => list.append(extractDeathDate(title, relation, claims(propertyId)))
                  case "CITY_OF_HEADQUARTERS" => list.append(extractHeadquarters(title, relation, claims(propertyId)))
                }
              }
            })
          }

          list.toList

        })
      })
      .flatMap(x => x)

    result.write.parquet(conf.output())

    spark.stop()

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