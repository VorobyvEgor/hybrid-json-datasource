package org.apache.spark.sql.hybrid

import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}
import org.apache.spark.sql.sources.{BaseRelation, CreatableRelationProvider, DataSourceRegister, RelationProvider, SchemaRelationProvider}
import org.apache.spark.sql.types.StructType

import java.util.UUID

class HybridJson extends CreatableRelationProvider
  with DataSourceRegister
  with RelationProvider
  with SchemaRelationProvider
  with Logging{

  override def createRelation(sqlContext: SQLContext,
                              mode: SaveMode,
                              parameters: Map[String, String],
                              data: DataFrame): BaseRelation = {
    val schema: StructType = data.schema

    log.info(s"Mode: ${mode}")
    log.info(s"Params: \n ${parameters.mkString("\n")} \n end params ----")
    log.info(s"Dataframe schema: ${schema.simpleString}")

    val path = parameters.getOrElse("path",
      throw new IllegalArgumentException("path must be specified !!!"))

    val objectName = parameters.getOrElse("objectName",
      throw new IllegalArgumentException("objectName must be specified !!!"))

    FileHelper.ensureDirectory(path)

    val rdd: RDD[InternalRow] = data.queryExecution.toRdd

    rdd.foreachPartition {
      p: Iterator[InternalRow] => {
        val converter = new RowConverter(schema)
        val res: Iterator[String] = converter.toJsonString(p)

        val fileName = path + "/" + UUID.randomUUID().toString + ".json"

        FileHelper.write(fileName, res)

      }

    }

    new BaseRelation {
      override def sqlContext: SQLContext = ???

      override def schema: StructType = ???
    }
  }

  override def shortName(): String = "hybrid-json"

  override def createRelation(sqlContext: SQLContext, parameters: Map[String, String]): BaseRelation = {
    throw new IllegalArgumentException("Schema must be set!")
  }

  override def createRelation(sqlContext: SQLContext, parameters: Map[String, String], schema: StructType): BaseRelation = {
    log.info(s"Params: \n ${parameters.mkString("\n")} \n end params ----")
    log.info(s"Dataframe schema: ${schema.simpleString}")

    val path = parameters.getOrElse("path",
      throw new IllegalArgumentException("path must be specified !!!"))

    new CsvRelation(schema, path)
  }

}
