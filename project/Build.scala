import sbt._
import sbt.Keys._
import net.virtualvoid.sbt.graph.Plugin._

object ProjectBuild extends Build {
  lazy val project = Project(
    id = "root",
    base = file("."),
    settings = Project.defaultSettings ++ graphSettings ++ Seq(
      organization := "com.tresata",
      name := "spark-columnar",
      version := "0.1.0-SNAPSHOT",
      scalaVersion := "2.10.4",
      crossScalaVersions := Seq("2.10.4", "2.11.5"),
      libraryDependencies <++= (scalaVersion) {
        case "2.10.4" => Seq("com.chuusai" % "shapeless_2.10.4" % "2.0.0")
        case "2.11.5" => Seq("com.chuusai" %% "shapeless" % "2.0.0")
      },
      libraryDependencies ++= Seq(
        "org.apache.spark" %% "spark-core" % "1.2.0" % "provided",
        "org.scalatest" %% "scalatest" % "2.2.1" % "test"
      )
    )
  )
}
