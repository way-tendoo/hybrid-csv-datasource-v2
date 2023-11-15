package org.apache.spark.sql.hybrid.csv.datasource

import org.apache.spark.sql.connector.catalog.Table
import org.apache.spark.sql.execution.datasources.FileFormat
import org.apache.spark.sql.execution.datasources.v2.FileDataSourceV2
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

class HybridCSVProvider extends FileDataSourceV2 {

  override def getTable(options: CaseInsensitiveStringMap): Table =
    throw new UnsupportedOperationException("hybrid-csv couldn't infer schema")

  override def getTable(options: CaseInsensitiveStringMap, schema: StructType): Table = {
    val paths     = getPaths(options)
    val tableName = getTableName(options, paths)
    HybridCSVTable(tableName, sparkSession, options, paths, Some(schema), fallbackFileFormat)
  }

  override def shortName(): String = "hybrid-csv"

  override def fallbackFileFormat: Class[_ <: FileFormat] = classOf[HybridCSVFileFormat]

}
