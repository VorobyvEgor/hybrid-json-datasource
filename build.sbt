ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.12.20"

libraryDependencies += "org.scalastic" %% "scalastic" % "3.2.11"
libraryDependencies += "org.scalatest" %% "scalatest" % "3.2.11"

libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.4.3"
libraryDependencies += "org.mongodb.scala" %% "mongo-scala-driver" % "2.9.0"

lazy val root = (project in file("."))
  .settings(
    name := "hybrid-json-datasource"
  )
