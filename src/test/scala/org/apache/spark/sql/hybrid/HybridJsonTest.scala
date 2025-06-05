package org.apache.spark.sql.hybrid

import org.apache.spark.internal.Logging
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should

class HybridJsonTest extends AnyFlatSpec
  with should.Matchers
  with Logging {

  val spark: SparkSession =
    SparkSession
      .builder()
      .master("local[1]")
      .appName("test")
      .getOrCreate()

  val options = Map(
    "path" -> "/Users/egorvorobev/Documents/advanced-spark-files",
    "objectName" -> "test01"
  )



  "Hybrid JSON" should "write" in {

    val df = spark.range(21, 41, 1, 2)

    df.write.format("hybrid-json")
      .options(options)
      .save()
  }

  "Hybrid JSON" should "read" in {

    val schema = StructType(
      StructField("id", LongType) :: Nil
    )

    val df = spark.read.format("hybrid-json")
      .options(options)
      .schema(schema)
      .load()

    df.printSchema
    df.show
    println(s"===============================${df.count}====================")
  }

}