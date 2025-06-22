package org.apache.spark.sql.hybrid

import org.apache.spark.sql.Dataset
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.JsonToStructs
import org.apache.spark.sql.catalyst.json.{JSONOptions, JacksonGenerator}
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}
import org.apache.spark.unsafe.types.UTF8String
import org.apache.spark.sql.functions.{broadcast, from_json}

import java.io.CharArrayWriter

class RowConverter(dataType: StructType) {

  private val intMinMax = scala.collection.mutable.Map[String, (Int, Int)]()

  // Initialize min-max for integer columns
  dataType.fields.foreach { field =>
    if (field.dataType == IntegerType) {
      intMinMax(field.name) = (Int.MaxValue, Int.MinValue)
    }
  }


  def toJsonString(input: Iterator[InternalRow]): Iterator[String] = {

    lazy val writer = new CharArrayWriter()

    lazy val gen = new JacksonGenerator(
      dataType, writer, new JSONOptions(Map.empty[String, String], defaultTimeZoneId = "UTC"))

    lazy val converter: Any => UTF8String = {
      def getAndReset(): UTF8String = {
        gen.flush()
        val json = writer.toString
        writer.reset()
        UTF8String.fromString(json)
      }

      dataType match {
        case _: StructType =>
          (row: Any) =>
            gen.write(row.asInstanceOf[InternalRow])
            updateMinMax(row.asInstanceOf[InternalRow]) // Update min-max for integer columns
            getAndReset()
      }
    }

    input.map {
      row => converter(row).toString
    }

  }

  private def updateMinMax(row: InternalRow): Unit = {
    dataType.fields.zipWithIndex.foreach { case (field, index) =>
      if (field.dataType == IntegerType) {
        val value = row.getInt(index)
        if (value < intMinMax(field.name)._1) {
          intMinMax(field.name) = (value, intMinMax(field.name)._2)
        }
        if (value > intMinMax(field.name)._2) {
          intMinMax(field.name) = (intMinMax(field.name)._1, value)
        }
      }
    }
  }

  def getMinMax: Map[String, (Int, Int)] = intMinMax.toMap


}
