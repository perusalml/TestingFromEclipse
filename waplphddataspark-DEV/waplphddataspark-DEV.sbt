name :="WAPL-PHD-Data-Streaming-DEV"

version :="33"

scalaVersion :="2.11.8"

val sparkVersion = "2.1.0"


libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion % "provided" exclude("org.slf4j", "slf4j-log4j12"),
  "org.apache.spark" %% "spark-streaming" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-sql" % sparkVersion % "provided",
  "org.apache.logging.log4j" % "log4j-api" % "2.6.1",
  "org.apache.logging.log4j" % "log4j-core" % "2.6.1",
  "org.apache.logging.log4j" % "log4j-slf4j-impl" % "2.6.1",
  
  "com.databricks" %% "spark-xml" % "0.4.1",
 "org.apache.spark" %% "spark-hive" % sparkVersion % "provided"
  )
  
 
  


