enablePlugins(JavaAppPackaging, UniversalPlugin, BuildInfoPlugin)

name := "spark-scala"

version := "0.1"

scalaVersion := "2.12.7"

parallelExecution in Test := false
fork in Test := true

libraryDependencies ++= {
  val akkaVersion = "2.5.23"
  val jacksonVersion = "2.12.7"
  val sparkVersion = "3.4.0"

  Seq(
    "org.scala-lang" % "scala-compiler" % scalaVersion.value,
    "org.scala-lang" % "scala-reflect" % scalaVersion.value,
    "org.scala-lang" % "scala-library" % scalaVersion.value,
    "org.apache.commons" % "commons-math3" % "3.6.1",
    "com.amazonaws" % "amazon-kinesis-client" % "1.10.0",
    "com.fasterxml.jackson.core" % "jackson-core" % jacksonVersion,
    "com.fasterxml.jackson.core" % "jackson-databind" % jacksonVersion,
    "com.fasterxml.jackson.module" %% "jackson-module-scala" % "2.12.7",
    "org.apache.hadoop" % "hadoop-client" % "2.8.5",
    "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
    "org.apache.spark" %% "spark-sql" % sparkVersion % "provided",
    "org.apache.spark" %% "spark-streaming" % sparkVersion % "provided",
    "org.apache.spark" %% "spark-mllib" % sparkVersion % "provided",
    "com.github.seratch" %% "awscala" % "0.8.2",
    "com.typesafe.scala-logging" %% "scala-logging" % "3.9.2",    
    "com.amazonaws" % "amazon-kinesis-client" % "1.10.0",
    "com.fasterxml.jackson.core" % "jackson-core" % jacksonVersion,
    "net.snowflake" %% "spark-snowflake" % "2.12.0-spark_3.4",
    "org.scalatest" %% "scalatest" % "3.2.0-SNAP10" % "test",
    "org.scalacheck" %% "scalacheck" % "1.14.0" % "test",
    "com.typesafe.akka" %% "akka-testkit" % akkaVersion % "test",
    "org.rogach" %% "scallop" % "3.3.2"

  )
}

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs@_*) => MergeStrategy.discard
  case x => MergeStrategy.first
}