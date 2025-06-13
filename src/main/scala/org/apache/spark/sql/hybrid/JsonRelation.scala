package org.apache.spark.sql.hybrid

import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SQLContext, SparkSession}
import org.apache.spark.sql.sources.{BaseRelation, TableScan}
import org.apache.spark.sql.types.{LongType, StructField, StructType}

class JsonRelation(inputSchema: StructType, files: Seq[(String, Long)])
  extends BaseRelation
    with TableScan
    with Logging{

  override def sqlContext: SQLContext = SparkSession.active.sqlContext

  override def schema: StructType = StructType(StructField("__commitMillis", LongType) :: Nil).merge(inputSchema)

  override def buildScan(): RDD[Row] = new JsonRDD(files, inputSchema).asInstanceOf[RDD[Row]]

  override def needConversion: Boolean = false
}
