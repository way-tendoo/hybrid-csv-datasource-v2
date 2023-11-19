package org.apache.spark.sql.hybrid.csv.datasource

import org.apache.spark.sql.SparkSession
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should

class HybridCSVBatchReadSpec extends AnyFlatSpec with should.Matchers {

  val spark: SparkSession = SparkSession.builder().master("local[1]").appName("test").getOrCreate()

  "read batch df" should "work" in {
    val users = spark.read.format("hybrid-csv").schema(User.Schema).load("./src/test/resources/users")

    users.show()
  }
}
