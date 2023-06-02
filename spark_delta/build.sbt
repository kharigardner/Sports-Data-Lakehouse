organization := "com.github.kharigardner"
name := "sports-lakehouse-spark-pipelines"
version := "0.1"

crossScalaVersions := Seq("2.11.12", "2.12.8")
scalaVersion := "2.12.8"

val sparkVersion = "3.3.2"

libraryDependencies += "org.apache.spark" %% "spark-sql" % sparkVersion % "provided"
libraryDependencies += "org.apache.spark" %% "spark-mllib" % sparkVersion % "provided"
librayDependencies += "org.apache.spark" %% "spark-streaming" % sparkVersion % "provided"
libraryDependencies += "org.apache.spark" %% "spark-sql-kafka-0-10" % sparkVersion % "provided"
libraryDependencies += "com.lihaoyi" %% "utest"
libraryDependencies += "com.lihaoyi" %% "os-lib"

homepage := Some(url("https://github.com/kharigardner/sports-data-lakehouse"))
developers

