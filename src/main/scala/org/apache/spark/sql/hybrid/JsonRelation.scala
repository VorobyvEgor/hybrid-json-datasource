package org.apache.spark.sql.hybrid

import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SQLContext, SparkSession}
import org.apache.spark.sql.sources.{BaseRelation, TableScan}
import org.apache.spark.sql.types.StructType

class JsonRelation(inputSchema: StructType, path: String)
  extends BaseRelation
    with TableScan
    with Logging{

  override def sqlContext: SQLContext = SparkSession.active.sqlContext

  override def schema: StructType = inputSchema

  override def buildScan(): RDD[Row] = new JsonRDD(path, schema).asInstanceOf[RDD[Row]]

  override def needConversion: Boolean = false
}
