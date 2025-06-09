package io.github.tiluan.chrononshim
package flink.utils

import ai.chronon.flink.FlinkSource
import org.apache.flink.connector.kafka.source.KafkaSource
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.api.common.typeinfo.TypeInformation

class ChrononFlinkKafkaEventSource[T : TypeInformation](kafkaSource: KafkaSource[T],
                                                        sourceName: String) extends FlinkSource[T] {

  override def getDataStream(topic: String, groupName: String)(
    env: StreamExecutionEnvironment, parallelism: Int
  ): DataStream[T] = {
    env.fromSource(
      kafkaSource,
      WatermarkStrategy.forMonotonousTimestamps(),
      sourceName,
    )
  }
}
