package io.dstlr.source

import io.dstlr.{Conf, DocumentRow}
import org.apache.spark.sql.{Dataset, SparkSession}

class ExampleDataSource(spark: SparkSession, conf: Conf) extends DataSource {

  val docs = Seq(
    "Barack Obama was born on August 4th, 1961.",
    "Barack Obama was born on August 4th, 1861.",
    "Good Technology is a company based in Sunnyvale.",
    "Good Technology is a company based in Cupertino.",
    "The International Arctic Research Center is located in Fairbanks, Alaska."
  )

  override def get(): Dataset[DocumentRow] = {

    import spark.implicits._

    spark.sparkContext.parallelize(docs)
      .zipWithIndex()
      .map(_.swap)
      .toDF("id", "contents")
      .as[DocumentRow]

  }
}