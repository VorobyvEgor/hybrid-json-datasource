package org.apache.spark.sql.hybrid

import org.apache.spark.{Partition, TaskContext}
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.StructType

class JsonRDD(path: String, schema: StructType)
  extends RDD[InternalRow](SparkSession.active.sparkContext, Nil)
    with Logging {

  override def compute(split: Partition, context: TaskContext): Iterator[InternalRow] = {
    val filePath = split.asInstanceOf[CsvPartition].path
    val file = scala.io.Source.fromFile(filePath)
    val lines: Iterator[String] = file.getLines()

    val parser = new JsonParser(schema)
    parser.toRow(lines)

  }

  override protected def getPartitions: Array[Partition] = {
    val files = FileHelper.getFiles(path)
    log.info(s"Files: \n ${files.map(_.getName).mkString("\n")} \n end file list ---")

    val parts: Array[CsvPartition] = files.map { f => f.getAbsolutePath }.zipWithIndex.map {
      case (f, i) => CsvPartition(i, f)
    }
    log.info(s"Partitions: ${parts.mkString(",")}")

    parts.asInstanceOf[Array[Partition]]
  }
}

case class CsvPartition(index: Int, path: String) extends Partition
