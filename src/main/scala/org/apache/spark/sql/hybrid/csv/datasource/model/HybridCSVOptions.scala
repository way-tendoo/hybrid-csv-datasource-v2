package org.apache.spark.sql.hybrid.csv.datasource.model

import java.nio.charset.Charset
import scala.util.Try

case class HybridCSVOptions(delimiter: String, charset: Charset, header: Boolean, separator: String)

object HybridCSVOptions {

  private object DefaultOptions {
    val Delimiter = ","
    val Header    = false
    val Charset   = "UTF-8"
    val Separator = "\n"
  }

  def apply(options: Map[String, String]): HybridCSVOptions = {
    val delimiter = options.getOrElse("delimiter", DefaultOptions.Delimiter)
    val header = options
      .get("header")
      .flatMap(tryCast(_.toBoolean))
      .getOrElse(DefaultOptions.Header)
    val charset =
      options.get("charset").flatMap(tryCast(Charset.forName)).getOrElse(Charset.forName(DefaultOptions.Charset))
    new HybridCSVOptions(delimiter, charset, header, DefaultOptions.Separator)
  }

  private def tryCast[R](cast: String => R)(value: String): Option[R] = Try(cast(value)).toOption
}
