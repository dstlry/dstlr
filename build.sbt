name := "dstlr"

version := "0.1"

scalaVersion := "2.11.12"

mainClass in assembly := Some("io.dstlr.Spark")

resolvers += "Restlet Repository" at "http://maven.restlet.org"
resolvers += "Spark Packages Repo" at "http://dl.bintray.com/spark-packages/maven"

// https://mvnrepository.com/artifact/com.lihaoyi/ujson
libraryDependencies += "com.lihaoyi" %% "ujson" % "0.7.1"

// https://mvnrepository.com/artifact/com.lucidworks.spark/spark-solr
libraryDependencies += "com.lucidworks.spark" % "spark-solr" % "3.6.0" exclude("org.slf4j", "slf4j-log4j12") exclude("org.apache.logging.log4j", "log4j-slf4j-impl")

// https://mvnrepository.com/artifact/com.softwaremill.sttp/core
libraryDependencies += "com.softwaremill.sttp" %% "core" % "1.5.11"

// https://mvnrepository.com/artifact/edu.stanford.nlp/stanford-corenlp
libraryDependencies += "edu.stanford.nlp" % "stanford-corenlp" % "3.9.2"
libraryDependencies += "edu.stanford.nlp" % "stanford-corenlp" % "3.9.2" classifier "models-english" classifier "models-english-kbp"

libraryDependencies += "neo4j-contrib" % "neo4j-spark-connector" % "2.1.0-M4"

// https://mvnrepository.com/artifact/org.apache.spark/spark-core
libraryDependencies += "org.apache.spark" %% "spark-core" % "2.4.0" exclude("org.slf4j", "slf4j-log4j12") exclude("org.apache.logging.log4j", "log4j-slf4j-impl")

// https://mvnrepository.com/artifact/org.jsoup/jsoup
libraryDependencies += "org.jsoup" % "jsoup" % "1.11.3"

// https://mvnrepository.com/artifact/org.neo4j.driver/neo4j-java-driver
libraryDependencies += "org.neo4j.driver" % "neo4j-java-driver" % "1.7.2"

// https://mvnrepository.com/artifact/org.rogach/scallop
libraryDependencies += "org.rogach" %% "scallop" % "3.1.5"

// https://mvnrepository.com/artifact/org.slf4j/slf4j-simple
libraryDependencies += "org.slf4j" % "slf4j-simple" % "1.7.25"

// https://mvnrepository.com/artifact/org.wikidata.wdtk/wdtk-wikibaseapi
libraryDependencies += "org.wikidata.wdtk" % "wdtk-wikibaseapi" % "0.9.0"

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}