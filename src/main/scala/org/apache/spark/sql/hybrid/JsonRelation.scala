package org.apache.spark.sql.hybrid

import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SQLContext, SparkSession}
import org.apache.spark.sql.sources.{BaseRelation, EqualTo, Filter, GreaterThan, GreaterThanOrEqual, IsNotNull, LessThan, LessThanOrEqual, PrunedFilteredScan, StringContains, TableScan}
import org.apache.spark.sql.types.{LongType, StructField, StructType}

import java.util

class JsonRelation(inputSchema: StructType, files: Seq[(String, Long, Seq[ColumnStat])])
  extends BaseRelation
    with TableScan
    with PrunedFilteredScan
    with Logging{

  override def sqlContext: SQLContext = SparkSession.active.sqlContext

  override def schema: StructType = StructType(StructField("__commitMillis", LongType) :: Nil).merge(inputSchema)

  override def buildScan(): RDD[Row] = new JsonRDD(files, inputSchema).asInstanceOf[RDD[Row]]

  override def needConversion: Boolean = false

  override def buildScan(requiredColumns: Array[String], filters: Array[Filter]): RDD[Row] = {
    log.info(s"--------------- call buildScan(requiredColumns: Array[String], filters: Array[Filter])")

    log.info(s"requiredColumns: ${requiredColumns.mkString(",")}")
    log.info(s"filters: ${filters.mkString(",")}")
    new PrunedFilteredJsonRDD(files, schema, filters)
      .asInstanceOf[RDD[Row]]
  }

  override def unhandledFilters(filters: Array[Filter]): Array[Filter] = {
    filters.filter {
      case _: EqualTo => false
      case _: GreaterThan => false
      case _: GreaterThanOrEqual => false
      case _: LessThan => false
      case _: LessThanOrEqual => false
      case _ => true
    }
  }
}
