package org.apache.spark.sql.hybrid

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.ExprUtils
import org.apache.spark.sql.catalyst.json.{CreateJacksonParser, JSONOptions, JacksonParser}
import org.apache.spark.sql.catalyst.util.{FailureSafeParser, PermissiveMode}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.unsafe.types.UTF8String


class JsonParser(schema: StructType) {

  def toRow(input: Iterator[String]): Iterator[InternalRow] = {

    val nullableSchema: StructType = schema.asNullable

    val nameOfCorruptRecord = SQLConf.get.getConf(SQLConf.COLUMN_NAME_OF_CORRUPT_RECORD)

    val parser: FailureSafeParser[UTF8String] = {
      val parsedOptions = new JSONOptions(Map.empty[String, String], defaultTimeZoneId = "UTC")
      val (parserSchema, actualSchema) = nullableSchema match {
        case s: StructType =>
          ExprUtils.verifyColumnNameOfCorruptRecord(s, nameOfCorruptRecord)
          (s, StructType(s.filterNot(_.name == nameOfCorruptRecord)))
        case other =>
          (StructType(Array(StructField("value", other))), other)
      }

      val rawParser = new JacksonParser(actualSchema, parsedOptions, allowArrayAsStructs = false)
      val createParser = CreateJacksonParser.utf8String _

      new FailureSafeParser[UTF8String](
        input => rawParser.parse(input, createParser, identity[UTF8String]),
        mode = PermissiveMode,
        parserSchema,
        columnNameOfCorruptRecord = nameOfCorruptRecord)
    }

    parser.parse(UTF8String.fromString(input.next()))

  }

}
