package org.apache.spark.sql.hybrid

import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.dsl.expressions.DslSymbol
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should

import scala.collection.immutable.Range.Int

class HybridJsonTest extends AnyFlatSpec
  with should.Matchers
  with Logging {

  val spark: SparkSession =
    SparkSession
      .builder()
      .master("local[1]")
      .appName("test")
      .getOrCreate()

  val optionsWrite = Map(
    "path" -> "/Users/egorvorobev/Documents/advanced-spark-files",
    "objectName" -> "test01"
  )

  val optionsRead = Map(
    "objectName" -> "test01"
  )



  "Hybrid JSON" should "write" in {

    val df = spark.range(21, 41, 1, 2)

    df.write.format("hybrid-json")
      .options(optionsWrite)
      .save()
  }

  "Hybrid JSON" should "read" in {

    val df = spark.read.format("hybrid-json")
      .options(optionsRead)
      .load()

    df.printSchema
    df.show
    println(s"===============================${df.count}====================")
  }

  "Hybrid JSON" should "write column stats" in {

    import spark.implicits._

    val df = spark
      .range(0, 20, 1, 1)
      .withColumn("id", 'id.cast("int"))
      .withColumn("id2", 'id * 2)

    df.show()
    df.printSchema()
    val schema = df.schema

    val intStructs: Seq[(StructField, Int)] = schema.zipWithIndex
      .filter { case (field, _) => field.dataType == IntegerType }

    log.info(s"parse Schema: ${intStructs.mkString("||")}")

    df.write.format("hybrid-json")
      .options(optionsWrite)
      .save()



  }

}