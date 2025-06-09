package io.github.tiluan.chrononshim
package flink.utils

import org.apache.flink.api.common.serialization.DeserializationSchema
import org.apache.flink.api.common.typeinfo.TypeInformation

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import java.nio.charset.StandardCharsets

class ChrononEventDeserializationSchema[T]()(implicit ti: TypeInformation[T]) extends DeserializationSchema[T] {

  @transient private lazy val objectMapper: ObjectMapper = {
    val mapper = new ObjectMapper()
    mapper.registerModule(DefaultScalaModule)
    mapper
  }

  override def deserialize(message: Array[Byte]): T = {
    if (message == null || message.length == 0) {
      throw new IllegalArgumentException("Input message is null or empty.")
    }
    try {
      objectMapper.readValue(message, ti.getTypeClass)
    } catch {
      case e: Exception =>
        val problematicMessage = new String(message, StandardCharsets.UTF_8)
        throw new RuntimeException(s"ChrononEventDeserializationSchema: Failed to deserialize event from JSON: '$problematicMessage'", e)
    }
  }

  override def isEndOfStream(nextElement: T): Boolean = false

  override def getProducedType: TypeInformation[T] = ti
}