package org.apache.spark.sql.hybrid.csv.datasource

import org.apache.hadoop.fs.FileStatus
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.datasources.{FileFormat, OutputWriterFactory}
import org.apache.spark.sql.hybrid.csv.datasource.sink.HybridCSVOutputWriterFactory
import org.apache.spark.sql.types.StructType

case class HybridCSVFileFormat() extends FileFormat {

  override def inferSchema(
    sparkSession: SparkSession,
    options: Map[String, String],
    files: Seq[FileStatus]
  ): Option[StructType] = None

  override def prepareWrite(
    sparkSession: SparkSession,
    job: Job,
    opts: Map[String, String],
    dataSchema: StructType
  ): OutputWriterFactory = new HybridCSVOutputWriterFactory(opts)
}
