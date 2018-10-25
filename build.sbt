name := "dynamo"

version := "0.1"

scalaVersion := "2.11.11"

libraryDependencies ++= Seq(
  "org.scalatest" %% "scalatest" % "3.0.4" % Test,
  "com.typesafe.akka" %% "akka-actor" % "2.5.14",
  "com.typesafe.akka" %% "akka-stream" % "2.5.14",
  "com.typesafe.akka" %% "akka-http" % "10.1.3",
  "ch.qos.logback" % "logback-classic" % "1.2.3",
  "com.gu" %% "scanamo" % "1.0.0-M7",
  "org.jsoup" % "jsoup" % "1.8.3"
)