package io.dstlr.source

import io.dstlr.DocumentRow
import org.apache.spark.sql.Dataset

trait DataSource {
  def get(): Dataset[DocumentRow]
}