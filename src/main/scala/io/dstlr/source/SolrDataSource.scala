package io.dstlr.source

import io.dstlr.{Conf, DocumentRow}
import org.apache.spark.sql.{Dataset, SparkSession}

class SolrDataSource(spark: SparkSession, conf: Conf) extends DataSource {

  override def get(): Dataset[DocumentRow] = {

    import spark.implicits._

    val options = Map(
      "collection" -> conf.solrIndex(),
      "query" -> conf.query(),
      "rows" -> conf.rows(),
      "zkhost" -> conf.solrUri()
    )

    // Create a DataFrame with the query results
    spark.read.format("solr")
      .options(options)
      .load()
      .as[DocumentRow]

  }
}