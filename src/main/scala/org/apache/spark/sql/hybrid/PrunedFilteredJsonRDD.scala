package org.apache.spark.sql.hybrid

import org.apache.spark.{Partition, TaskContext}
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.sources.{EqualTo, Filter, GreaterThan, GreaterThanOrEqual, LessThan, LessThanOrEqual}
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}

import scala.io.BufferedSource

class PrunedFilteredJsonRDD(files: Seq[(String, Long, Seq[ColumnStat])],
                            schema: StructType,
                            resultScehma: StructType,
                            requiredColumns: Array[String],
                            filters: Array[Filter])
  extends RDD[InternalRow](SparkSession.active.sparkContext, Nil)
    with Logging {

  override def compute(split: Partition, context: TaskContext): Iterator[InternalRow] = {
    val filePath: String = split.asInstanceOf[JsonPartition].path
    val fileWriteTime: Long = split.asInstanceOf[JsonPartition].writeTime
    val columnStat: Seq[ColumnStat] = split.asInstanceOf[JsonPartition].columnStat

    var readCondition: Boolean = true

    val fltMap: Any = columnStat.filter((colStat: ColumnStat) => filters.flatMap {
      case EqualTo(attribute, _) =>
        Some(attribute)
      case GreaterThan(attribute, _) =>
        Some(attribute)
      case GreaterThanOrEqual(attribute, _) =>
        Some(attribute)
      case LessThan(attribute, _) =>
        Some(attribute)
      case LessThanOrEqual(attribute, _) =>
        Some(attribute)
      case _ => None
    }.contains(colStat.name))
      .foreach((c: ColumnStat) => {
        filters.foreach {
          case EqualTo(attribute, value) =>
            val parsedValue: Int = parseValue(value)
            if (attribute == c.name && c.min <= parsedValue && c.max >= parsedValue) readCondition = true
          case GreaterThan(attribute, value) =>
            val parsedValue: Int = parseValue(value)
            if (attribute == c.name && c.max < parsedValue) readCondition = true
          case GreaterThanOrEqual(attribute, value) =>
            val parsedValue: Int = parseValue(value)
            if (attribute == c.name && c.max <= parsedValue) readCondition = true
          case LessThan(attribute, value) =>
            val parsedValue: Int = parseValue(value)
            if (attribute == c.name && c.min > parsedValue) readCondition = true
          case LessThanOrEqual(attribute, value) =>
            val parsedValue: Int = parseValue(value)
            if (attribute == c.name && c.min >= parsedValue) readCondition = true
          case _ =>
        }
      })

    //    if (readCondition) {
    val file: BufferedSource = scala.io.Source.fromFile(filePath)
    val lines: Iterator[String] = file.getLines()

    val parser = new JsonParser(schema)
    parser.toRow(lines).map(
      f = (line: InternalRow) => {
        val res = Seq.empty[AnyRef]
        log.info(s"Line for parsing: ${line.toString}\n")
        val rowPrep: InternalRow = InternalRow.fromSeq(Seq(fileWriteTime) ++ line.toSeq(schema))
        val requiredSchema: Seq[AnyRef] = resultScehma.zipWithIndex.filter { case (filed, index) =>
          requiredColumns.contains(filed.name)
        }.map { case (fileld_, i) =>
          rowPrep.get(i, fileld_.dataType)
        }
        //        val row = InternalRow.fromSeq(Seq(rowPrep.get(1, IntegerType)))
        val row = InternalRow.fromSeq(requiredSchema)
        log.info(s"Row for reading: ${row.toString}\n")
        row
      }
    )
    //    } else null

  }

  override protected def getPartitions: Array[Partition] = {

    log.info(s"Files: \n ${files.mkString("\n")} \n end file list ---")

    val filesForRead: Seq[(String, Long, Seq[ColumnStat])] = files.filter { case (filePath, commitMillis, colStat) =>
      var readCondition: Boolean = true
      colStat.filter { stat =>
        filters.flatMap {
          case EqualTo(attribute, _) =>
            Some(attribute)
          case GreaterThan(attribute, _) =>
            Some(attribute)
          case GreaterThanOrEqual(attribute, _) =>
            Some(attribute)
          case LessThan(attribute, _) =>
            Some(attribute)
          case LessThanOrEqual(attribute, _) =>
            Some(attribute)
          case _ => None
        }.contains(stat.name)
      }.foreach((c: ColumnStat) => {
        filters.foreach {
          case EqualTo(attribute, value) =>
            val parsedValue: Int = parseValue(value)
            if (attribute == c.name && (c.min > parsedValue || c.max < parsedValue)) readCondition = false
          case GreaterThan(attribute, value) =>
            val parsedValue: Int = parseValue(value)
            if (attribute == c.name && c.max >= parsedValue) readCondition = false
          case GreaterThanOrEqual(attribute, value) =>
            val parsedValue: Int = parseValue(value)
            if (attribute == c.name && c.max < parsedValue) readCondition = false
          case LessThan(attribute, value) =>
            val parsedValue: Int = parseValue(value)
            if (attribute == c.name && c.min >= parsedValue) readCondition = false
          case LessThanOrEqual(attribute, value) =>
            val parsedValue: Int = parseValue(value)
            if (attribute == c.name && c.min < parsedValue) readCondition = false
          case _ =>
        }
      })
      log.info(s"Read this part $filePath condition = $readCondition")
      readCondition
    }

    val parts: Array[JsonPartition] = filesForRead.zipWithIndex.map {
      case (f, i) => JsonPartition(i, f._1, f._2, f._3)
    }.toArray
    log.info(s"Partitions: ${parts.mkString(",")}")

    parts.asInstanceOf[Array[Partition]]
  }

  private def parseValue(value: Any): Int = {
    val newValue = value match {
      case v: Long => v.toInt // Convert Long to Integer if needed
      case v: Int => v
      case _ => throw new IllegalArgumentException("Value must be an Integer or Long")
    }
    log.info(s"Parsed value for Filter = $newValue")
    newValue

  }
}

//case class JsonPartition(index: Int, path: String, writeTime: Long, columnStat: Seq[ColumnStat]) extends Partition
