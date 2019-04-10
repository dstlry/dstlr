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
      .getOrCreate()

    // Delete old output directory
    FileSystem.get(spark.sparkContext.hadoopConfiguration).delete(new Path(conf.output()), true)

    // Start time
    val start = System.currentTimeMillis()

    val result_acc = spark.sparkContext.longAccumulator("results")
    val dirty_acc = spark.sparkContext.longAccumulator("dirty")

    Neo4j(spark.sparkContext)
      .cypher("MATCH (d:Document)-->(s:Mention)-->(r:Relation {type: \"CITY_OF_HEADQUARTERS\"})-->(o:Mention) MATCH (s)-->(e:Entity)-->(f:Fact {relation: r.type}) RETURN d, s, r, o, e, f")
      .partitions(conf.partitions())
      .loadNodeRdds
      .map(row => {

        result_acc.add(1)

        val doc = row.get(0).asInstanceOf[InternalNode]
        val sub = row.get(1).asInstanceOf[InternalNode]
        val rel = row.get(2).asInstanceOf[InternalNode]
        val obj = row.get(3).asInstanceOf[InternalNode]
        val ent = row.get(4).asInstanceOf[InternalNode]
        val fact = row.get(5).asInstanceOf[InternalNode]

        val docId = doc.get("id").asString()
        val indexes = obj.get("index").asList()
        val observed = obj.get("span").asString()
        val truth = fact.get("value").asString()

        val dirty = observed != truth

        if (dirty) {
          dirty_acc.add(1)
        }

        (docId, indexes, observed, truth, dirty)

      })
      .filter(row => row._5)
      .saveAsTextFile(conf.output())

    val duration = System.currentTimeMillis() - start
    println(s"Received ${result_acc.value} results and cleaned ${dirty_acc.value} in ${duration}ms @ ${result_acc.value / (duration / 1000)} result/s")

    spark.stop()

  }
}