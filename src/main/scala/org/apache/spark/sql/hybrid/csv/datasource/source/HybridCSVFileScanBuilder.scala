package org.apache.spark.sql.hybrid.csv.datasource.source

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.connector.read.Scan
import org.apache.spark.sql.execution.datasources.PartitioningAwareFileIndex
import org.apache.spark.sql.execution.datasources.v2.FileScanBuilder
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

case class HybridCSVFileScanBuilder(
  spark: SparkSession,
  fileIndex: PartitioningAwareFileIndex,
  schema: StructType,
  options: CaseInsensitiveStringMap)
    extends FileScanBuilder(spark, fileIndex, schema) {

  override def build(): Scan = {
    HybridCSVScan(
      spark,
      fileIndex,
      schema,
      readDataSchema(),
      StructType { Nil },
      partitionFilters,
      dataFilters,
      options
    )
  }
}
