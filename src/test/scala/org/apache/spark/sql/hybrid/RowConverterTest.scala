package org.apache.spark.sql.hybrid

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.{Dataset, SparkSession}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should

import java.lang

class RowConverterTest extends AnyFlatSpec with should.Matchers {

  val spark: SparkSession =
    SparkSession
      .builder()
      .master("local[1]")
      .appName("test")
      .getOrCreate()

  "Converter" should "work" in {
    val data: Dataset[lang.Long] = spark.range(1)
    val rows: Iterator[InternalRow] = data.queryExecution.toRdd.collect().toIterator
    val converter: RowConverter = new RowConverter(data.schema)
    val out: Iterator[String] = converter.toJsonString(rows)
    out.toList.foreach(println)
  }
}
