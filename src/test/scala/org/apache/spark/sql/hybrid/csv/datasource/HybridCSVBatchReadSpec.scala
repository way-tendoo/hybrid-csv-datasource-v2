package org.apache.spark.sql.hybrid.csv.datasource

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, lit}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should

class HybridCSVBatchReadSpec extends AnyFlatSpec with should.Matchers {

  val spark: SparkSession = SparkSession.builder().master("local[1]").appName("test").getOrCreate()

  "read batch df" should "work" in {
    val users =
      spark.read
        .format("hybrid-csv")
        .schema(User.Schema)
        .option("header", value = true)
        .load("./src/test/resources/users")
        .filter(col("age") >= lit(18))
        .select("firstName")

    users.explain(true)

    users.show()
  }
}
