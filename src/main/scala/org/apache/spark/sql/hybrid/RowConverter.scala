package org.apache.spark.sql.hybrid

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.json.{JSONOptions, JacksonGenerator}
import org.apache.spark.sql.types.StructType
import org.apache.spark.unsafe.types.UTF8String

import java.io.CharArrayWriter

class RowConverter(dataType: StructType) {
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
            getAndReset()
      }
    }

    Iterator(converter(input.next()).toString)
  }
}
