package org.apache.spark.sql.hybrid

import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
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



  "Hybrid JSON" should s"write" in {

    val df = spark.range(0, 10, 1, 1)
    val options = Map(
      "path" -> "/Users/egorvorobev/Documents/advanced-spark-files",
      "objectName" -> "test01"
    )

    df.write.format("hybrid-json")
      .options(options)
      .save()
  }

}