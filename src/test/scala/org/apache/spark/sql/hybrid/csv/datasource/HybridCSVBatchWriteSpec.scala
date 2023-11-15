package org.apache.spark.sql.hybrid.csv.datasource

import org.apache.spark.sql.SparkSession
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should

class HybridCSVBatchWriteSpec extends AnyFlatSpec with should.Matchers {

  val spark: SparkSession = SparkSession.builder().master("local[1]").appName("test").getOrCreate()

  import spark.implicits._

  "write batch df" should "work" in {
    val df = Seq(("Maksim", "Kazantsev", 28), ("Ivan", "Ivanov", 23)).toDF("name", "surname", "age")

    df.write.format("hybrid-csv").mode("overwrite").option("header", value = false).save("./src/main/resources/users")
  }

}
