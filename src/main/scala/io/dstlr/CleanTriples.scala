package io.dstlr

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.SparkSession
import org.neo4j.driver.internal.InternalNode
import org.neo4j.spark.Neo4j

/**
  * Enrich the "LINKS_TO" relationships of our extracted triples using data from WikiData.
  */
object CleanTriples {

  def main(args: Array[String]): Unit = {

    val conf = new Conf(args)
    println(conf.summary)

    val spark = SparkSession
      .builder()
      .appName("dstlr - EnrichTriples")
      .config("spark.neo4j.bolt.url", conf.neoUri())
      .config("spark.neo4j.bolt.user", conf.neoUsername())
      .config("spark.neo4j.bolt.password", conf.neoPassword())
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    // Delete old output directory
    FileSystem.get(spark.sparkContext.hadoopConfiguration).delete(new Path(conf.output()), true)

    val mapping = spark.read.option("header", "true").csv("wikidata.csv").as[WikiDataMappingRow]

    Neo4j(spark.sparkContext)
      .cypher("MATCH (d:Document)-->(s:Entity)-->(r:Relation {type: \"CITY_OF_HEADQUARTERS\"})-->(o:Entity) MATCH (s)-->(u:URI)-->(w:WikiDataValue {relation: r.type}) RETURN d, s, r, o, u, w")
      .loadNodeRdds
      .foreach(row => {

        val doc = row.get(0).asInstanceOf[InternalNode]
        val sub = row.get(1).asInstanceOf[InternalNode]
        val rel = row.get(2).asInstanceOf[InternalNode]
        val obj = row.get(3).asInstanceOf[InternalNode]
        val uri = row.get(4).asInstanceOf[InternalNode]
        val wdv = row.get(5).asInstanceOf[InternalNode]

        val observed = obj.get("label").asString()
        val truth = wdv.get("value").asString()

        if (observed == truth) {
          println("Clean")
        } else {
          println("Dirty")
        }
      })

    spark.stop()

  }
}