package io.dstlr

import java.text.SimpleDateFormat
import java.util.Date

import com.softwaremill.sttp._
import org.apache.spark.sql.SparkSession
import ujson.Value

/**
  * Enrich the "LINKS_TO" relationships of our extracted triples using data from WikiData.
  */
object EnrichTriples {

  val dateFormat = new SimpleDateFormat("'+'yyyy-MM-dd'T'HH:mm:ss'Z'")

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("dstlr - EnrichTriples")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    val mapping = spark.sparkContext.broadcast(
      spark.read.option("header", "true").csv("wikidata.csv").as[WikiDataMappingRow].rdd.map(row => (row.property, row.relation)).collectAsMap()
    )

    val entities = spark.read.parquet("triples").as[TripleRow]
      .filter($"relation" === "LINKS_TO" && $"objectValue".isNotNull)
      .select($"objectValue")
      .distinct()
      .filter(row => row.getString(0).contains("Barack"))
      .coalesce(1)

    entities.show()

    entities.foreachPartition(part => {

      // Standard HTTP backend
      implicit val backend = HttpURLConnectionBackend()

      // WikiData API can take 50 titles at a time
      part.grouped(50).foreach(batch => {

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
                case "DATE_OF_BIRTH" => println(extractBirthDate(claims(propertyId)))
                case "DATE_OF_DEATH" => println(extractDeathDate(claims(propertyId)))
                case "CITY_OF_HEADQUARTERS" => println(extractHeadquarters(claims(propertyId)))
              }
            }
          })
        }
      })
    })

    def extractBirthDate(json: Value): Date = {
      dateFormat.parse(json(0)("mainsnak")("datavalue")("value")("time").str)
    }

    def extractDeathDate(json: Value): Date = {
      dateFormat.parse(json(0)("mainsnak")("datavalue")("value")("time").str)
    }

    def extractHeadquarters(json: Value): String = {
      null
    }

    spark.stop()

  }
}