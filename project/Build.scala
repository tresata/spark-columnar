import sbt._
import sbt.Keys._
import net.virtualvoid.sbt.graph.Plugin._
import sbtassembly.Plugin._
import sbtassembly.Plugin.AssemblyKeys._

object ProjectBuild extends Build {
  lazy val project = Project(
    id = "root",
    base = file("."),
    settings = Project.defaultSettings ++ graphSettings ++ assemblySettings ++ Seq(
      name := "spark-columnar",
      version := "0.1-SNAPSHOT",
      scalaVersion := "2.10.4",
      retrieveManaged := true,
      libraryDependencies ++= Seq(
        "com.chuusai" % "shapeless_2.10.4" % "2.0.0",
        "org.apache.spark" %% "spark-core" % "1.0.0" % "provided",
        //"org.slf4j" % "slf4j-api" % "1.6.6" % "provided",
        //"org.slf4j" % "slf4j-log4j12" % "1.6.6" % "test",
        "org.scalatest" %% "scalatest" % "1.9.2" % "test"
      )
    )
  )
}
