package org.apache.spark.sql.hybrid.csv.datasource

import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapreduce.TaskAttemptContext
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.datasources.{CodecStreams, OutputWriter}
import org.apache.spark.sql.hybrid.csv.datasource.model.HybridCSVOptions
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

import java.lang.{Long => JLong}

class HybridCSVOutputWriter(
  val path: String,
  schema: StructType,
  context: TaskAttemptContext,
  options: HybridCSVOptions)
    extends OutputWriter {

  private val writer = CodecStreams.createOutputStreamWriter(context, new Path(path), options.charset)

  if (options.header) {
    val header = schema.map(_.name).mkString(options.delimiter)
    writer.write(header)
    writer.write(options.separator)
  }

  override def write(row: InternalRow): Unit = {
    val output = row
      .toSeq(schema)
      .zip(schema)
      .map { case (value, sf) => value -> sf.dataType }
      .map(rowToString)
      .mkString(options.delimiter)
    writer.write(output)
    writer.write(options.separator)
  }

  private def rowToString(field: (Any, DataType)): String = field match {
    case (null, _)                               => ""
    case (jInt: Integer, _: IntegerType)         => jInt.toString
    case (jLong: JLong, _: LongType)             => jLong.toString
    case (utf8String: UTF8String, _: StringType) => utf8String.toString
    case (value, dataType) =>
      throw new UnsupportedOperationException(s"Value: $value of type ${dataType.simpleString} is not supported!")
  }

  override def close(): Unit = writer.close()
}
