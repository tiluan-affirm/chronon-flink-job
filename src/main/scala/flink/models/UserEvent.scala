package io.github.tiluan.chrononshim
package flink.models

case class UserEvent(user_id: String, email: String, is_fraud: Boolean, created_at: Long)
