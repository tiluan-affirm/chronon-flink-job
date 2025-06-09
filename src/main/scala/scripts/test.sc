import ai.chronon.online.Fetcher.Request
import io.github.tiluan.chrononshim.online_impl.mongo.ChrononMongoOnlineImpl

import scala.concurrent.Await
import scala.concurrent.duration.DurationInt

val conf: Map[String, String] = Map.apply(
  "user" -> "admin",
  "password" -> "admin",
  "host" -> "localhost",
  "port" -> "27017"
)
val api = new ChrononMongoOnlineImpl(conf)

val fetcher = api.buildFetcher()
val future = fetcher.fetchGroupBys(Seq(Request("quickstart.purchases.v1",
  Map("user_id" -> "tian000"))))
Await.result(future, 10.seconds)