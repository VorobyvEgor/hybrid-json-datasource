package org.apache.spark.sql.hybrid

import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
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

  "Hybrid JSON" should "parse schema" in {

    val schemaStr1 = "{\"type\":\"struct\",\"fields\":[{\"name\":\"id\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}}]}"
//
    val schemaStr2 = "{\"type\":\"struct\",\"fields\":[{\"name\":\"value\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}}]}"
//
    val schemaCommit = StructType(StructField("__commitMillis", LongType) :: Nil)
//
    val schemas: Seq[String] = Seq(schemaStr1, schemaStr2)
//
    val schema: StructType = schemas.map(str => StructType.fromString(str)).reduce(_ merge _)
//
////    val schema = StructType.fromString(schemaStr)
//
//    schema.printTreeString()
//
    val resultSchema: StructType = schemaCommit.merge(schema)
    resultSchema.printTreeString()
//
//    val df_mil: RDD[Row] = spark.sparkContext.parallelize(Seq(Row(737383.toLong)))
//    val df_id: RDD[Row] = SparkSession.active.sparkContext.parallelize(Seq(Row(7)))
//
//    spark.createDataFrame(df_mil.++(df_id), schemaCommit).show

    val row1 = InternalRow.fromSeq(Seq(88494940, 7, "test"))

    println(InternalRow.fromSeq(Seq(83838) ++ row1.toSeq(resultSchema)))

//    spark.createDataFrame(spark.sparkContext.parallelize(Seq(InternalRow(FileIndex("test1", "where", 3993939)))))



  }

}