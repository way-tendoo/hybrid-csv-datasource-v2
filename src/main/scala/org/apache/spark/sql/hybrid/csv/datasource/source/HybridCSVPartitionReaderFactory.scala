package org.apache.spark.sql.hybrid.csv.datasource.source

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.csv.{CSVOptions, UnivocityParser}
import org.apache.spark.sql.catalyst.util.FailureSafeParser
import org.apache.spark.sql.connector.read.PartitionReader
import org.apache.spark.sql.execution.datasources.v2.{FilePartitionReaderFactory, PartitionReaderFromIterator}
import org.apache.spark.sql.execution.datasources.{HadoopFileLinesReader, PartitionedFile}
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.SerializableConfiguration

class HybridCSVPartitionReaderFactory(
  schema: StructType,
  options: CSVOptions,
  conf: Broadcast[SerializableConfiguration])
    extends FilePartitionReaderFactory {
  override def buildReader(file: PartitionedFile): PartitionReader[InternalRow] = {
    val parser      = new UnivocityParser(schema, options)
    val linesReader = new HadoopFileLinesReader(file, conf.value.value)
    val lines = linesReader.map { line =>
      new String(line.getBytes, 0, line.getLength, parser.options.charset)
    }
    val safeParser = new FailureSafeParser[String](
      input => parser.parse(input),
      parser.options.parseMode,
      schema,
      parser.options.columnNameOfCorruptRecord
    )
    val ir = lines.flatMap(safeParser.parse)
    new PartitionReaderFromIterator(ir)
  }
}
