package org.apache.spark.sql.hybrid.csv.datasource

import org.apache.spark.sql.SparkSession
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should

class HybridCSVBatchWriteSpec extends AnyFlatSpec with should.Matchers {

  val spark: SparkSession = SparkSession.builder().master("local[1]").appName("test").getOrCreate()

  import spark.implicits._

  "write batch df with schema" should "work" in {
    val users = User.Users.toDF()
    users.write
      .format("hybrid-csv")
      .mode("overwrite")
      .option("header", value = true)
      .save("./src/test/resources/users")
  }
}
