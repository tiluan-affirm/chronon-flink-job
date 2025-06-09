package io.github.tiluan.chrononshim
package online_impl.mongo

import ai.chronon.aggregator.windowing.ResolutionUtils
import ai.chronon.online.{GroupByServingInfoParsed, KVStore}
import ai.chronon.online.KVStore.{GetResponse, TimedValue}
import flink.utils.Utils.{fetchGroupByServingInfoParsed, loadGroupByServingInfoParsedFromString}

import org.mongodb.scala.model.{Filters, Indexes, ReplaceOptions}
import org.mongodb.scala.model.Indexes.{ascending, compoundIndex}
import org.mongodb.scala.{Document, MongoClient, MongoCommandException}

import scala.collection.mutable
import scala.concurrent.duration.DurationInt
import scala.concurrent.{Await, Future}
import scala.util.{Failure, Try}

class MongoKvStore (mongoClient: MongoClient, databaseName: String) extends KVStore{
  private val metaDataStore: mutable.Map[String, GroupByServingInfoParsed] =
    mutable.Map.empty[String, GroupByServingInfoParsed]

  private def getGroupByServingInfoParsed(groupByName: String): GroupByServingInfoParsed =
    metaDataStore.getOrElseUpdate(groupByName,
      fetchGroupByServingInfoParsed(groupByName, databaseName, mongoClient, Constants.mongoKey, Constants.mongoValue))

  private def getTileSizeInMillsForGroupBy(groupByServingInfoParsed: GroupByServingInfoParsed): Long =
    ResolutionUtils.getSmallestWindowResolutionInMillis(groupByServingInfoParsed.groupBy).getOrElse(1L)

  override def create(dataset: String): Unit = {
    val database = mongoClient.getDatabase(databaseName)
    val collection = database.getCollection(dataset)

    try {
      Await.result(database.createCollection(dataset).toFuture(), 10.seconds) // 设置合理的超时
      println(s"Collection '$dataset' creation initiated successfully.")
    } catch {
      case e: MongoCommandException if e.getErrorCode == 48 /* NamespaceExists */ =>
        println(s"Collection '$dataset' already exists. No action taken for creation.")
      case e: Exception =>
        println(s"Error creating collection '$dataset': ${e.getMessage}")
        throw e
    }

    try {
      val tsIndexName = Await.result(
        collection.createIndex(ascending(Constants.mongoTs)).toFuture(),
        10.seconds
      )
      println(s"Index '$tsIndexName' on ${Constants.mongoTs} created for collection '$dataset'.")
    } catch {
      case e: Exception =>
        println(s"Error creating index on ${Constants.mongoTs} for collection '$dataset': ${e.getMessage}")
        throw e
    }

    try {
      val compoundIndexName = Await.result(
        collection.createIndex(
          compoundIndex(
            ascending(Constants.mongoKey),
            ascending(Constants.mongoTs)
          )
        ).toFuture(),
        10.seconds
      )
      println(s"Compound index '$compoundIndexName' created for collection '$dataset'.")
    } catch {
      case e: Exception =>
        println(s"Error creating compound index for collection '$dataset': ${e.getMessage}")
        throw e
    }
  }

  override def multiGet(requests: Seq[KVStore.GetRequest]): Future[Seq[KVStore.GetResponse]] = {
    val futures = requests.map{ request =>
      val collection = mongoClient.getDatabase(databaseName).getCollection(request.dataset)
      val dataset = request.dataset
      val groupByServingInfoParsed = getGroupByServingInfoParsed(dataset.substring(0, dataset.lastIndexOf('_')))
      val afterTsMills = request.afterTsMillis
      val baseFilter = afterTsMills match {
        case Some(afterTsMills) =>
          Filters.and(
            Filters.equal(Constants.mongoKey, request.keyBytes),
            Filters.gte(Constants.mongoTs, afterTsMills)
          )
        case None =>
          Filters.equal(Constants.mongoKey, request.keyBytes)
      }

      val filter = if (groupByServingInfoParsed.isTilingEnabled) {
        val tileSize = getTileSizeInMillsForGroupBy(groupByServingInfoParsed)

        // 1. Constants.mongoTileSize not exists
        // OR
        // 2. Constants.mongoTileSize exists and it equals to tileSize
        val tileSizeCondition = Filters.or(
          Filters.exists(Constants.mongoTileSize, exists = false),
          Filters.equal(Constants.mongoTileSize, tileSize)
        )

        Filters.and(
          baseFilter,
          tileSizeCondition
        )
      } else {
        baseFilter
      }

      collection
        .find(filter)
        .sort(Indexes.ascending(Constants.mongoTs))
        .toFuture()
        .map { documents =>
          if (documents.isEmpty) {
            GetResponse(request, Failure(new NoSuchElementException(s"Key not found with TS: $afterTsMills")))
          } else {
            GetResponse(request, Try(documents.map( document =>
              TimedValue(document.get(Constants.mongoValue).get.asBinary().getData,
                document.get(Constants.mongoTs).get.asInt64().longValue()))))
          }
        }
    }
    Future.sequence(futures)
  }

  override def multiPut(keyValueDatasets: Seq[KVStore.PutRequest]): Future[Seq[Boolean]] = {
    val futures = keyValueDatasets.map{ putRequest =>
      val collection = mongoClient.getDatabase(databaseName).getCollection(putRequest.dataset)
      val dataset = putRequest.dataset
      val groupByServingInfoParsed = getGroupByServingInfoParsed(dataset.substring(0, dataset.lastIndexOf('_')))

      if (groupByServingInfoParsed.isTilingEnabled) {
        val tsMills = putRequest.tsMillis.get
        val tileSize = getTileSizeInMillsForGroupBy(groupByServingInfoParsed)
        val windowStart = (tsMills / tileSize) * tileSize

        val filter = Filters.and(
          Filters.equal(Constants.mongoKey, putRequest.keyBytes),
          Filters.equal(Constants.mongoTileSize, tileSize),
          Filters.equal(Constants.mongoTs, windowStart),
          Filters.equal(Constants.mongoTileSize, tileSize)
        )
        val document = Document(
          Constants.mongoKey -> putRequest.keyBytes,
          Constants.mongoValue -> putRequest.valueBytes,
          Constants.mongoTs -> windowStart,
          Constants.mongoTileSize -> tileSize,
        )

        collection.replaceOne(
          filter,
          document,
          ReplaceOptions().upsert(true)
        ).toFuture().map(result => result.wasAcknowledged()).recover { case _ => false }
      } else {
        val document = Document(
          Constants.mongoKey -> putRequest.keyBytes,
          Constants.mongoValue -> putRequest.valueBytes,
          Constants.mongoTs -> putRequest.tsMillis
        )

        collection.insertOne(document).toFuture().map(result => result.wasAcknowledged()).recover { case _ => false }
      }
    }
    Future.sequence(futures)
  }

  override def bulkPut(sourceOfflineTable: String, destinationOnlineDataSet: String, partition: String): Unit = ???
}
