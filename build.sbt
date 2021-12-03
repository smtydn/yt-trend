import Dependencies._

ThisBuild / scalaVersion     := "2.13.7"
ThisBuild / version          := "0.1.0-SNAPSHOT"
ThisBuild / organization     := "com.example"
ThisBuild / organizationName := "example"

lazy val sparkVersion = "3.2.0"

lazy val root = (project in file("."))
  .settings(
    name := "Trending YouTube Video Statistics",
    libraryDependencies += scalaTest % Test
  )

libraryDependencies += "org.apache.spark" %% "spark-sql" % sparkVersion