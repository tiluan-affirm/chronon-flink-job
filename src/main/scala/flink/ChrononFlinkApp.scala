package io.github.tiluan.chrononshim
package flink

import ai.chronon.api.{GroupBy, ThriftJsonCodec}
import flink.utils.Utils.{buildKafkaSource, loadGroupByServingInfoParsedFromString}

import ai.chronon.api.Extensions.{GroupByOps, SourceOps}
import models.{PurchaseEvent, UserEvent}
import ai.chronon.online.{GroupByServingInfoParsed, KVStore}
import online_impl.mongo.ChrononMongoOnlineImpl

import ai.chronon.flink.{AsyncKVStoreWriter, FlinkJob, WriteResponse}
import flink.utils.ChrononFlinkKafkaEventSource

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction
import org.apache.flink.streaming.api.scala._
import org.apache.spark.sql.Encoders
import org.slf4j.LoggerFactory

import scala.reflect.runtime.universe.TypeTag

object ChrononFlinkApp {
  private val log = LoggerFactory.getLogger(getClass)

  def main(args: Array[String]): Unit = {
    if (args.length != 1) {
      log.error("Usage: ChrononFlinkApp <COMPILED_GROUP_BY_FILE>")
      System.exit(1)
    }

    val configPath = args(0)
    log.debug(s"Loading GroupBy from $configPath")

    try {
      val groupBy = ThriftJsonCodec.fromJsonFile[GroupBy](configPath, check = true)
      log.debug(s"GroupBy: $groupBy")
      val topic = groupBy.streamingSource.get.topic
      log.debug(s"Target Topic: $topic")

      val onlineImplConf = Map(
        "user" -> "admin",
        "password" -> "admin",
        "host" -> "localhost",
        "port" -> "27017"
      )
      val api = new ChrononMongoOnlineImpl(onlineImplConf)
      val writeFn = new AsyncKVStoreWriter(api, groupBy.metaData.name)

      val batchDataset = groupBy.metaData.name.replaceAll("[^a-zA-Z0-9_]", "_").toUpperCase + "_BATCH"
      log.debug(s"GroupBy $groupBy Batch Dataset: $batchDataset")
      val serializedGroupByServingInfo = api.genKvStore.getString(
        ai.chronon.api.Constants.GroupByServingInfoKey,
        batchDataset,
        1000, // Set 1000ms for timeout
      ).get

      val groupByServingInfoParsed = loadGroupByServingInfoParsedFromString(serializedGroupByServingInfo)
      val execEnv = StreamExecutionEnvironment.getExecutionEnvironment

      // Dispatch based on the topic string to the correct type-safe function
      topic match {
        case "events.purchases.1" =>
          runJob[PurchaseEvent](execEnv, topic, groupByServingInfoParsed, writeFn)

        case "events.users.1" =>
          runJob[UserEvent](execEnv, topic, groupByServingInfoParsed, writeFn)

        case _ =>
          log.error(s"No job configuration found for topic: $topic")
          System.exit(1)
      }

      // Start the Flink job execution
      execEnv.execute(s"Chronon Flink Job for topic: $topic")

    } catch {
      case e: Exception =>
        log.error("Failed to run Chronon Flink App", e)
        System.exit(1)
    }
  }

  private def runJob[T <: Product : TypeInformation : TypeTag](
                                                      env: StreamExecutionEnvironment,
                                                      topic: String,
                                                      groupByServingInfoParsed: GroupByServingInfoParsed,
                                                      writeFn: RichAsyncFunction[KVStore.PutRequest, WriteResponse],
                                                    ): Unit = {
    log.debug(s"Setting up Flink job for type [${implicitly[TypeInformation[T]]}] on topic [$topic]")

    val evtSource = new ChrononFlinkKafkaEventSource[T](
      buildKafkaSource[T](topic),
      s"KafkaSource-$topic"
    )

    new FlinkJob[T](
      eventSrc = evtSource,
      sinkFn = writeFn,
      groupByServingInfoParsed = groupByServingInfoParsed,
      encoder = Encoders.product[T],
      parallelism = 2,
    ).runTiledGroupByJob(env)
  }
}