package io.dstlr

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.SparkSession
import org.neo4j.driver.internal.InternalNode

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
      .getOrCreate()

    // Delete old output directory
    FileSystem.get(spark.sparkContext.hadoopConfiguration).delete(new Path(conf.output()), true)

    org.neo4j.spark.Neo4j(spark.sparkContext)
      .cypher("MATCH (e1:Entity)<-[:DATE_OF_BIRTH]-(e2:Entity)-[:LINKS_TO]->(u:URI)-[:DATE_OF_BIRTH]->(w:WikiDataValue) RETURN e1, e2, u, w")
      .loadNodeRdds
      .foreach(row => {
        println(row.get(0).asInstanceOf[InternalNode].asMap())
      })

    spark.stop()

  }
}