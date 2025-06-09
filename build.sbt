ThisBuild / version := "0.1.0-SNAPSHOT"
ThisBuild / scalaVersion := "2.12.15"

addSbtPlugin("com.eed3si9n" % "sbt-assembly" % "2.1.1")

val chrononVersion = "0.0.93"
val chrononDependencies = Seq[ModuleID](
  "ai.chronon" %% "flink" % chrononVersion,
  "ai.chronon" %% "online" % chrononVersion,
  "ai.chronon" %% "api" % chrononVersion,
)

val sparkVersion = "3.1.1"
val sparkDependencies = Seq[ModuleID]()

val flinkVersion = "1.16.1"
val flinkDependencies = Seq[ModuleID](
  "org.apache.flink" % "flink-core" % flinkVersion % "provided",
  "org.apache.flink" %% "flink-streaming-scala" % flinkVersion % "provided",
  "org.apache.flink" % "flink-connector-kafka" % flinkVersion,
  "org.apache.flink" % "flink-connector-kinesis" % flinkVersion
)

val jacksonVersion = "2.16.2"
val jacksonDependencies = Seq[ModuleID](
  "com.fasterxml.jackson.module" %% "jackson-module-scala" % jacksonVersion,
)

val mongoDependencies = Seq[ModuleID](
  "org.mongodb.spark" %% "mongo-spark-connector" % "10.5.0",
  "org.mongodb.scala" %% "mongo-scala-driver" % "5.5.0"
)

val allDependencies = {
  flinkDependencies ++ sparkDependencies ++ chrononDependencies ++ jacksonDependencies ++ mongoDependencies
}

lazy val root = (project in file("."))
  .settings(
    name := "chronon-shim",
    idePackagePrefix := Some("io.github.tiluan.chrononshim"),

    libraryDependencies ++= allDependencies,
    libraryDependencies += "org.slf4j" % "slf4j-api" % "2.0.17",
    libraryDependencies += "ch.qos.logback" % "logback-classic" % "1.5.13",
  )
