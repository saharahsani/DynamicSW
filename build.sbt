name := "DynamicSW"

version := "0.1"

scalaVersion := "2.12.12"
// https://mvnrepository.com/artifact/org.apache.spark/spark-core
libraryDependencies += "org.apache.spark" %% "spark-core" % "3.0.1"

// https://mvnrepository.com/artifact/org.apache.spark/spark-mllib
libraryDependencies += "org.apache.spark" %% "spark-mllib" % "3.0.1"

// https://mvnrepository.com/artifact/org.apache.spark/spark-sql
libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.0.1"
// https://mvnrepository.com/artifact/org.apache.kafka/kafka
libraryDependencies += "org.apache.kafka" %% "kafka" % "2.6.0"

// https://mvnrepository.com/artifact/org.apache.kafka/kafka-clients
libraryDependencies += "org.apache.kafka" % "kafka-clients" % "2.6.0"

// https://mvnrepository.com/artifact/org.apache.spark/spark-streaming-kafka-0-10
libraryDependencies += "org.apache.spark" %% "spark-streaming-kafka-0-10" % "3.0.1"

// https://mvnrepository.com/artifact/org.scalanlp/breeze
libraryDependencies += "org.scalanlp" %% "breeze" % "1.0"

// https://mvnrepository.com/artifact/org.slf4j/slf4j-simple
/*
libraryDependencies += "org.slf4j" % "slf4j-simple" % "1.7.30"*/
