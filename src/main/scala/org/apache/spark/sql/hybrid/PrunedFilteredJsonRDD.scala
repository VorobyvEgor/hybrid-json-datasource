package org.apache.spark.sql.hybrid

import org.apache.spark.{Partition, TaskContext}
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.StructType

import scala.io.BufferedSource

class PrunedFilteredJsonRDD(files: Seq[(String, Long, Seq[Map[String, Any]])],
                            schema: StructType,
                            filters: Array[Filter])
  extends RDD[InternalRow](SparkSession.active.sparkContext, Nil)
    with Logging {

  override def compute(split: Partition, context: TaskContext): Iterator[InternalRow] = {
    val filePath: String = split.asInstanceOf[JsonPartition].path
    val fileWriteTime: Long = split.asInstanceOf[JsonPartition].writeTime
    val columnStat: Seq[Map[String, Any]] = split.asInstanceOf[JsonPartition].columnStat

    val file: BufferedSource = scala.io.Source.fromFile(filePath)
    val lines: Iterator[String] = file.getLines()

    val parser = new JsonParser(schema)
    parser.toRow(lines).map(
      (line: InternalRow) => {
        log.info(s"Line for parsing: ${line.toString}\n")
        val row = InternalRow.fromSeq(Seq(fileWriteTime) ++ line.toSeq(schema))
        log.info(s"Row for reading: ${row.toString}\n")
        row
      }
    )

  }

  override protected def getPartitions: Array[Partition] = {

    log.info(s"Files: \n ${files.mkString("\n")} \n end file list ---")

    val parts: Array[JsonPartition] = files.zipWithIndex.map {
      case (f, i) => JsonPartition(i, f._1, f._2, f._3)
    }.toArray
    log.info(s"Partitions: ${parts.mkString(",")}")

    parts.asInstanceOf[Array[Partition]]
  }
}

//case class JsonPartition(index: Int, path: String, writeTime: Long, columnStat: Seq[ColumnStat]) extends Partition
