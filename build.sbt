ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "3.1.2"
val akkaVersion = "2.6.18"

scalacOptions ++= Seq(
  "-feature",
  "-deprecation",
  "-encoding", "UTF-8",
  "-unchecked"
)

libraryDependencies ++= Seq(
  "com.typesafe.akka" %% "akka-actor" % akkaVersion,
  "com.typesafe.akka" %% "akka-testkit" % akkaVersion % Test,
  "org.scalameta" %% "munit" % "0.7.26" % Test
)

lazy val root = (project in file("."))
  .settings(
    name := "AkkaFLow"
  )


libraryDependencies += "org.scala-lang" % "scala-library" % "2.13.8"
libraryDependencies += "org.scala-lang" % "scala-compiler" % "2.13.8"
libraryDependencies += "org.scala-lang.modules" %% "scala-parser-combinators" % "2.1.1"
libraryDependencies += "io.spray" %%  "spray-json" % "1.3.6"
