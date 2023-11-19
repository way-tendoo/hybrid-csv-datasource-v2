package org.apache.spark.sql.hybrid.csv.datasource

import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

case class User(firstName: String, lastName: String, age: Int)

object User {

  val Users: Seq[User] = Seq(User("Maksim", "Kazantsev", 28), User("Ivan", "Ivanov", 22))

  val Schema: StructType = StructType {
    StructField("firstName", StringType) :: StructField("lastName", StringType) :: StructField("age", IntegerType) :: Nil
  }
}
