package org.apache.spark.sql.hybrid.csv.datasource.source

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.csv.CSVOptions
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.connector.read.PartitionReaderFactory
import org.apache.spark.sql.execution.datasources.PartitioningAwareFileIndex
import org.apache.spark.sql.execution.datasources.v2.TextBasedFileScan
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.util.SerializableConfiguration

import java.util.TimeZone
import scala.collection.JavaConverters._

case class HybridCSVScan(
  sparkSession: SparkSession,
  fileIndex: PartitioningAwareFileIndex,
  dataSchema: StructType,
  readDataSchema: StructType,
  readPartitionSchema: StructType,
  partitionFilters: Seq[Expression],
  dataFilters: Seq[Expression],
  options: CaseInsensitiveStringMap)
    extends TextBasedFileScan(sparkSession, options) {

  override def createReaderFactory(): PartitionReaderFactory = {
    val columnPruning             = sparkSession.sessionState.conf.csvColumnPruning
    val columnNameOfCorruptRecord = sparkSession.sessionState.conf.columnNameOfCorruptRecord
    val caseSensitiveMap          = options.asCaseSensitiveMap.asScala.toMap
    val csvOptions =
      new CSVOptions(caseSensitiveMap, columnPruning, TimeZone.getDefault.getID, columnNameOfCorruptRecord)
    val hadoopConf    = sparkSession.sessionState.newHadoopConfWithOptions(caseSensitiveMap)
    val broadcastConf = sparkSession.sparkContext.broadcast(new SerializableConfiguration(hadoopConf))
    new HybridCSVPartitionReaderFactory(dataSchema, readDataSchema, csvOptions, broadcastConf)
  }
}
