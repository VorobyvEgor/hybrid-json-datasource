package org.apache.spark.sql.hybrid

import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}
import org.apache.spark.sql.sources.{BaseRelation, CreatableRelationProvider, DataSourceRegister, RelationProvider, SchemaRelationProvider}
import org.apache.spark.sql.types.StructType
import org.bson.codecs.configuration.{CodecRegistries, CodecRegistry}
import org.mongodb.scala.{MongoClient, MongoDatabase}
import org.mongodb.scala.MongoClient.DEFAULT_CODEC_REGISTRY

import java.util.UUID

class HybridJson extends CreatableRelationProvider
  with DataSourceRegister
  with RelationProvider
  with SchemaRelationProvider
  with Logging {

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

//        val test: Long = p.reduce((row1, row2) => Math.max(row1.getLong(1), row2.getLong(2)))

        MongoConnection.writeFileIndex(
          collectionName = "file_index",
          objectName = objectName,
          path = fileName
        )

        println(s"Info about $fileName has written to Mongo with object $objectName")

      }
    }

    MongoConnection.writeSchemaIndex(
      collectionName = "schema_index",
      objectName = objectName,
      schemaRef = schema.asNullable.json
    )

    log.info(s"Schema info has written for object $objectName to Mongo")

    new BaseRelation {
      override def sqlContext: SQLContext = ???

      override def schema: StructType = ???
    }
  }

  override def shortName(): String = "hybrid-json"

  override def createRelation(sqlContext: SQLContext, parameters: Map[String, String]): BaseRelation = {
    log.info(s"Params: \n ${parameters.mkString("\n")} \n end params ----")

    val objectName = parameters.getOrElse("objectName",
      throw new IllegalArgumentException("objectName must be specified !!!"))

    val (files, schemaRef) = MongoConnection.readInfo(objectName = objectName)

    new JsonRelation(schemaRef, files)
  }

  override def createRelation(sqlContext: SQLContext, parameters: Map[String, String], schema: StructType): BaseRelation = {
    ???
  }

}
