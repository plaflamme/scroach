name := "scroach"

lazy val main = project.in(file(".")).configs(ScalaBuff)

scalabuffSettings

libraryDependencies ++= Seq(
  "com.twitter" %% "finagle-httpx" % "6.24.0",
  "org.scalatest" %% "scalatest" % "2.2.2" % "test",
  "org.scalacheck" %% "scalacheck" % "1.12.1" % "test"
)

fork in run := true

