package org.apache.spark.sql.hybrid.csv.datasource

import org.apache.hadoop.fs.FileStatus
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.connector.read.ScanBuilder
import org.apache.spark.sql.connector.write.{ LogicalWriteInfo, Write, WriteBuilder }
import org.apache.spark.sql.execution.datasources.FileFormat
import org.apache.spark.sql.execution.datasources.v2.FileTable
import org.apache.spark.sql.hybrid.csv.datasource.sink.HybridCSVWrite
import org.apache.spark.sql.hybrid.csv.datasource.source.HybridCSVFileScanBuilder
import org.apache.spark.sql.types._
import org.apache.spark.sql.util.CaseInsensitiveStringMap

case class HybridCSVTable(
  name: String,
  spark: SparkSession,
  options: CaseInsensitiveStringMap,
  paths: Seq[String],
  userSpecifiedSchema: Option[StructType],
  fallbackFileFormat: Class[_ <: FileFormat])
    extends FileTable(spark, options, paths, userSpecifiedSchema) {

  override def inferSchema(files: Seq[FileStatus]): Option[StructType] = userSpecifiedSchema

  override def newWriteBuilder(info: LogicalWriteInfo): WriteBuilder =
    new WriteBuilder {
      override def build(): Write = HybridCSVWrite(name, paths, supportsDataType, info)
    }

  override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder = {
    HybridCSVFileScanBuilder(spark, fileIndex, schema, options)
  }

  override def supportsDataType(dataType: DataType): Boolean = dataType match {
    case _: IntegerType | _: LongType | _: StringType => true
    case _                                            => false
  }

  override def formatName: String = "csv"
}
