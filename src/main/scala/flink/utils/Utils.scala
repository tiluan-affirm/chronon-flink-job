package io.github.tiluan.chrononshim
package flink.utils

import ai.chronon.api.Extensions.{WindowOps, WindowUtils}
import ai.chronon.api.{GroupByServingInfo, PartitionSpec, ThriftJsonCodec}
import ai.chronon.online.GroupByServingInfoParsed
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.connector.kafka.source.KafkaSource
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer
import org.mongodb.scala.MongoClient
import org.mongodb.scala.model.Filters

import java.util.UUID
import scala.concurrent.Await
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.DurationInt

object Utils {
  def buildKafkaSource[T : TypeInformation](topic: String): KafkaSource[T] = KafkaSource.builder[T]()
      .setBootstrapServers("localhost:9092")
      .setTopics(topic)
      .setGroupId(UUID.randomUUID().toString)
      .setStartingOffsets(OffsetsInitializer.latest())
      .setValueOnlyDeserializer(new ChrononEventDeserializationSchema[T])
      .build()

  def loadGroupByServingInfoParsedFromString(serializedGroupByServingInfo: String): GroupByServingInfoParsed = {
    val groupByServingInfo = ThriftJsonCodec
      .fromJsonStr[GroupByServingInfo](serializedGroupByServingInfo, check = true, classOf[GroupByServingInfo])
    val partitionSpec = PartitionSpec(format = "yyyy-MM-dd", spanMillis = WindowUtils.Day.millis)
    new GroupByServingInfoParsed(groupByServingInfo, partitionSpec)
  }

  def fetchGroupByServingInfoParsed(groupByName: String, databaseName: String, mongoClient: MongoClient,
                                    mongoKey: String, mongoValue: String): GroupByServingInfoParsed = {
    val collectionName = groupByName.replaceAll("[^a-zA-Z0-9_]", "_").toUpperCase + "_BATCH"
    val collection = mongoClient.getDatabase(databaseName).getCollection(collectionName)
    val filter = Filters.equal(mongoKey,
      ai.chronon.api.Constants.GroupByServingInfoKey.getBytes(ai.chronon.api.Constants.UTF8))

    val metaDataStringFuture = collection
      .find(filter)
      .limit(1)
      .toFuture()
      .map { documents =>
        if (documents.isEmpty) {
          throw new NoSuchElementException(
            s"GroupByServingInfoParsed Not Found for ${groupByName.replaceAll("[^a-zA-Z0-9_]", "_") + "_BATCH"}")
        } else {
          new String(documents.headOption.get(mongoValue).asBinary().getData, ai.chronon.api.Constants.UTF8)
        }
      }

    val metaDataString = Await.result(metaDataStringFuture, 10.seconds)
    loadGroupByServingInfoParsedFromString(metaDataString)
  }
}
