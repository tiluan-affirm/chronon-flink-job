package io.github.tiluan.chrononshim
package flink.models

case class PurchaseEvent(purchase_id: String, user_id: String, product_id: String, purchase_price: Int, ts: Long)
