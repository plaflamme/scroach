import com.trueaccord.scalapb.{ScalaPbPlugin => PB}

name := "scroach"

PB.protobufSettings

PB.runProtoc in PB.protobufConfig := (args =>
  com.github.os72.protocjar.Protoc.runProtoc("-v261" +: args.toArray))

PB.flatPackage in PB.protobufConfig := true

lazy val main = project.in(file("."))

resolvers += bintray.Opts.resolver.mavenRepo("plaflamme")

libraryDependencies ++= Seq(
  "com.twitter" %% "finagle-httpx" % "6.24.0",
  "org.scalatest" %% "scalatest" % "2.2.2" % "test",
  "org.scalacheck" %% "scalacheck" % "1.12.1" % "test"
)

def whenCircleBuild[T](f: => T): Seq[T] = sys.env.get("CIRCLE_BUILD_NUM").map { _ => f }.toSeq

// Disable tests tagged with SlowTest during circle-ci builds
testOptions in Test ++= whenCircleBuild(Tests.Argument("-l", "SlowTest"))

testOptions in Test += Tests.Setup { () => "src/test/scripts/local_cluster.sh start" ! }

fork in run := true

