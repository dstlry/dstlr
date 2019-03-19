package io.dstlr

import com.softwaremill.sttp._
import org.apache.spark.sql.SparkSession

/**
  * Enrich the "LINKS_TO" relationships of our extracted triples using data from WikiData.
  */
object EnrichTriples {

  // TODO: Give list of properties to extract.

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("dstlr - EnrichTriples")
      .master("local[*]")
      .getOrCreate()

    val sc = spark.sparkContext

    import spark.implicits._

    val ds = spark.read.option("header", "true").csv("triples").as[TripleRow]
    ds.show(false)

    val mapping = sc.broadcast(sc.textFile("wikidata.csv").map(row => {
      val split = row.split(",")
      (split(0), split(1))
    }).collectAsMap())

    ds.filter("relation = 'LINKS_TO' AND objectValue != 'null'").coalesce(1).foreach(row => {

      implicit val backend = HttpURLConnectionBackend()

      // Send the request to WikiData
      val resp = sttp.get(uri"https://www.wikidata.org/w/api.php?action=wbgetentities&sites=enwiki&titles=${row.objectValue}&languages=en&format=json").send()

      // Parse JSON response
      val json = ujson.read(resp.unsafeBody)

      val entities = json("entities")

      println(row.objectValue)
      entities.obj.foreach(entity => {
        val claims = entity._2("claims")
        mapping.value.foreach(pair => {
          if (claims.obj.contains(pair._1)) {
            println(s"${pair._1} -> ${pair._2}")
            println(claims(pair._1)(0)("mainsnak")("datavalue")("value")("time").str.split("T")(0))
          }
        })
      })
      println()

    })

    // https://www.wikidata.org/w/api.php?action=wbgetentities&sites=enwiki&titles=Berlin&languages=en&format=json

    /**
      *
      * val sort: Option[String] = None
      * val query = "http language:scala"
      *
      * // the `query` parameter is automatically url-encoded
      * // `sort` is removed, as the value is not defined
      * val request = sttp.get(uri"https://api.github.com/search/repositories?q=$query&sort=$sort")
      *
      * implicit val backend = HttpURLConnectionBackend()
      * val response = request.send()
      *
      */

//    implicit val backend = HttpURLConnectionBackend()
//
//    val ent = "Barack_Obama"
//
//    val request = sttp.get(uri"https://www.wikidata.org/w/api.php?action=wbgetentities&sites=enwiki&titles=$ent&languages=en&format=json")
//
//    val response = request.send()

  }

}