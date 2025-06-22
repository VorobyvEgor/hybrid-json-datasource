package org.apache.spark.sql.hybrid

import org.apache.commons.collections.collection.TypedCollection
import org.apache.spark.Success
import org.apache.spark.internal.Logging
import org.apache.spark.sql.Column
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.expressions.{ExpectsInputTypes, Expression, Nondeterministic, UnaryExpression}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.types.{AbstractDataType, DataType, StringType, StructField, StructType, TypeCollection}
import org.apache.spark.unsafe.types.UTF8String
import org.bson.codecs.configuration.{CodecRegistries, CodecRegistry}
import org.mongodb.scala.MongoClient.DEFAULT_CODEC_REGISTRY
import org.mongodb.scala._
import org.mongodb.scala.bson.{Document, _}
import org.mongodb.scala.bson.codecs.Macros._
import org.mongodb.scala.bson.conversions.Bson
import org.mongodb.scala.model.{Filters, UpdateOptions}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.DurationInt
import scala.concurrent.{Await, Future}
import org.apache.spark.sql.functions._
import org.mongodb.scala.result.UpdateResult

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import org.mongodb.scala.model.Filters._

case class ColumnStat(name: String, min: Int, max: Int)

case class FileIndex(objectName: String, path: String, commitMillis: Long, columnStats: Seq[ColumnStat])

case class SchemaIndex(objectName: String, schemaRef: String, commitMillis: Long)

object MongoConnection extends Logging {


    private val uri: String = "mongodb://192.168.1.42:27017"
//  private val uri = System.getenv("MONGO_URI")

  @transient lazy val mongoClient: MongoClient = MongoClient(uri)
  @transient lazy val mongoDatabase: MongoDatabase = mongoClient.getDatabase("index_store")


  def writeFileIndex(collectionName: String,
                     objectName: String,
                     path: String,
                     columnStat: Seq[ColumnStat]) = {

    val codec: CodecRegistry = CodecRegistries.fromRegistries(
      CodecRegistries.fromProviders(classOf[FileIndex]), DEFAULT_CODEC_REGISTRY)

    val dataBase: MongoDatabase = mongoDatabase.withCodecRegistry(codec)

    @transient lazy val fileCollection: MongoCollection[FileIndex] =
      dataBase.getCollection(collectionName = collectionName)

    log.info(s"ColumnStats for writing: ${columnStat.mkString("||")}")

    fileCollection.insertOne(FileIndex(objectName, path, System.currentTimeMillis(), columnStat)).subscribe(
      (result: Completed) => log.info("Document inserted"),
      (e: Throwable) => log.info(s"Failed with error: ${e.getMessage}"),
      () => log.info("Insertion complete")
    )

  }

  def writeSchemaIndex(collectionName: String,
                       objectName: String,
                       schemaRef: String) = {

    val codec: CodecRegistry = CodecRegistries.fromRegistries(
      CodecRegistries.fromProviders(classOf[SchemaIndex]), DEFAULT_CODEC_REGISTRY)

    val dataBase: MongoDatabase = mongoDatabase.withCodecRegistry(codec)

    @transient lazy val schemaCollection: MongoCollection[SchemaIndex] =
      dataBase.getCollection(collectionName = collectionName)

    val filter: Bson = Filters.and(
      Filters.eq(fieldName = "objectName", objectName), Filters.eq(fieldName = "schemaRef", schemaRef)
    )

    val test: Future[Option[SchemaIndex]] = schemaCollection.find(filter).headOption()

    //    if (Await.result(test, 30.seconds).isDefined)  {
    log.info(s"Start write new Schema!!!! Row for insert: ${SchemaIndex(objectName = objectName, schemaRef = schemaRef, commitMillis = System.currentTimeMillis()).toString}")
    schemaCollection.insertOne(SchemaIndex(objectName = objectName, schemaRef = schemaRef, commitMillis = System.currentTimeMillis()))
      .subscribe((result: Completed) => log.info(s"Schema inserted. ${result.toString()}"),
        (e: Throwable) => log.info(s"Failed with error: ${e.getMessage}"),
        () => log.info("Insertion complete")
      )
    //    } else {
    //      log.info(s"Schema with objectName: $objectName and schemaRef: $schemaRef already exists!!!")
    //    }

  }

  def readInfo(objectName: String): (Seq[(String, Long)], StructType) = {

    val codecFile: CodecRegistry = CodecRegistries.fromRegistries(
      CodecRegistries.fromProviders(classOf[FileIndex]), DEFAULT_CODEC_REGISTRY)

    val codecSchema: CodecRegistry = CodecRegistries.fromRegistries(
      CodecRegistries.fromProviders(classOf[SchemaIndex]), DEFAULT_CODEC_REGISTRY)

    @transient lazy val fileCollection: MongoCollection[FileIndex] =
      mongoDatabase.withCodecRegistry(codecFile).getCollection(collectionName = "file_index")

    @transient lazy val schemaCollection: MongoCollection[SchemaIndex] =
      mongoDatabase.withCodecRegistry(codecSchema).getCollection(collectionName = "schema_index")

    val filter: Bson = Filters.eq("objectName", objectName)

    val filesFuture: Future[Seq[(String, Long)]] = fileCollection.find(filter).collect().toFuture().map(
      (rows: Seq[FileIndex]) => rows.map((row: FileIndex) => {
        (row.path, row.commitMillis)
      })
    )
    val files: Seq[(String, Long)] = Await.result(filesFuture, 30.seconds)

    log.info(s"Files for read ${files.mkString(", ")}")

    val schemaFuture: Future[Seq[String]] = schemaCollection.find(filter).collect().toFuture().map(
      (rows: Seq[SchemaIndex]) => rows.map((row: SchemaIndex) => row.schemaRef)
    )

    val schemas: Seq[String] = Await.result(schemaFuture, 30.seconds)

    log.info(s"Schemas for read: ${schemas.mkString("||||")}")

    val schema: StructType = schemas.map(
      (schemaStr: String) => StructType.fromString(schemaStr)
    ).reduce(_ merge _)

    log.info(s"Result Schema for read: ${schema.printTreeString()}")

    (files, schema)
  }


}