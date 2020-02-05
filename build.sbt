name := "AprioriMinal"

organization := "com.minal.spark"

version := "1.0"

scalaVersion := "2.11.8"

libraryDependencies += "org.apache.spark" %% "spark-core" % "2.1.0" % "provided"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.1.0" % "provided"
libraryDependencies += "org.apache.spark" %% "spark-mllib" % "2.1.0" % "provided"
libraryDependencies += "org.log4s" %% "log4s" % "1.3.3" % "provided"

mainClass in assembly := Some("com.minal.spark.Main")

assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)
