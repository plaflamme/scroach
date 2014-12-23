name := "scroach"

lazy val main = project.in(file(".")).configs(ScalaBuff)

scalabuffSettings

libraryDependencies ++= Seq(
  "com.twitter" %% "finagle-httpx" % "6.24.0"
)

fork in run := true

