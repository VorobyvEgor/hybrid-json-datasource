package org.apache.spark.sql.hybrid

import org.apache.commons.collections.collection.TypedCollection
import org.apache.spark.Success
import org.apache.spark.internal.Logging
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

import java.{lang, util}
import scala.collection.JavaConverters
import scala.collection.JavaConverters.asScalaBufferConverter

case class ColumnStat(name: String, min: Int, max: Int)

case class FileIndex(objectName: String, path: String, commitMillis: Long, columnStats: Seq[Map[String, Any]])

case class SchemaIndex(objectName: String, schemaRef: String, commitMillis: Long)

object MongoConnection extends Logging {


//    private val uri: String = "mongodb://192.168.1.42:27017"
  private val uri = System.getenv("MONGO_URI")

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

    fileCollection.insertOne(FileIndex(objectName, path, System.currentTimeMillis()
      , columnStat.map(stat => {
        Map(
          "name" -> stat.name,
          "min" -> stat.min,
          "max" -> stat.max
        )
      }))
    ).subscribe(
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

  def readInfo(objectName: String): (Seq[(String, Long, Seq[ColumnStat])], StructType) = {

    val codecFile: CodecRegistry = CodecRegistries.fromRegistries(
      CodecRegistries.fromProviders(classOf[ColumnStat]),
      CodecRegistries.fromProviders(classOf[FileIndex]), DEFAULT_CODEC_REGISTRY)

    val codecSchema: CodecRegistry = CodecRegistries.fromRegistries(
      CodecRegistries.fromProviders(classOf[SchemaIndex]), DEFAULT_CODEC_REGISTRY)

    @transient lazy val fileCollection: MongoCollection[Document] =
      mongoDatabase
//        .withCodecRegistry(codecFile)
        .getCollection(collectionName = "file_index")

    @transient lazy val schemaCollection: MongoCollection[SchemaIndex] =
      mongoDatabase.withCodecRegistry(codecSchema).getCollection(collectionName = "schema_index")

    val filter: Bson = Filters.eq("objectName", objectName)

    val filesFuture: Future[Seq[(String, Long, Seq[ColumnStat])]] = fileCollection.find(filter).collect().toFuture().map(
      (rows: Seq[Document]) => rows.map((row: Document) => {
        val path: String = row.getString("path")
        val commitMillis: Long = row.getLong("commitMillis")
        val columnStatsBson: Seq[BsonDocument] = row.get("columnStats").get.asInstanceOf[BsonArray].getValues.asScala.map { bsonValue =>
          bsonValue.asDocument() // Предполагаем, что каждый элемент - это BsonDocument
        }

        val colStat: Seq[ColumnStat] = columnStatsBson.map(
          (doc: BsonDocument) => {
            val name = doc.getString("name").getValue
            val min = doc.getInteger("min")
            val max = doc.getInteger("max")
            ColumnStat(name = name, min = min, max = max)
          }
        )

        (path, commitMillis, colStat)
      })
    )

    val files: Seq[(String, Long, Seq[ColumnStat])] = Await.result(filesFuture, 30.seconds)

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