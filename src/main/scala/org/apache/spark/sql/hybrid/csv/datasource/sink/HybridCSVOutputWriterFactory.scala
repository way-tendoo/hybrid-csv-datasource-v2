package org.apache.spark.sql.hybrid.csv.datasource.sink

import org.apache.hadoop.mapreduce.TaskAttemptContext
import org.apache.spark.sql.execution.datasources.{OutputWriter, OutputWriterFactory}
import org.apache.spark.sql.hybrid.csv.datasource.model.HybridCSVOptions
import org.apache.spark.sql.types.StructType

class HybridCSVOutputWriterFactory(opts: Map[String, String]) extends OutputWriterFactory {
  override def getFileExtension(context: TaskAttemptContext): String = ".csv"

  override def newInstance(path: String, dataSchema: StructType, context: TaskAttemptContext): OutputWriter = {
    val options = HybridCSVOptions(opts)
    new HybridCSVOutputWriter(path, dataSchema, context, options)
  }
}
