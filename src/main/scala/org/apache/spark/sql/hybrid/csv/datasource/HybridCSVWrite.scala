package org.apache.spark.sql.hybrid.csv.datasource

import org.apache.hadoop.mapreduce.{ Job, TaskAttemptContext }
import org.apache.spark.sql.connector.write.LogicalWriteInfo
import org.apache.spark.sql.execution.datasources.{ OutputWriter, OutputWriterFactory }
import org.apache.spark.sql.execution.datasources.v2.FileWrite
import org.apache.spark.sql.hybrid.csv.datasource.model.HybridCSVOptions
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{ DataType, StructType }

case class HybridCSVWrite(
  formatName: String,
  paths: Seq[String],
  supportsDataType: DataType => Boolean,
  info: LogicalWriteInfo)
    extends FileWrite {

  override def prepareWrite(
    sqlConf: SQLConf,
    job: Job,
    opts: Map[String, String],
    dataSchema: StructType
  ): OutputWriterFactory = new HybridCSVOutputWriterFactory(opts)
}
