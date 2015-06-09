resolvers += Resolver.url("bintray-sbt-plugin-releases", url("http://dl.bintray.com/content/sbt/sbt-plugin-releases"))(Resolver.ivyStylePatterns)

resolvers += Resolver.url("plaflamme-sbt-plugin-releases", url("http://dl.bintray.com/content/plaflamme/sbt-plugin-releases"))(Resolver.ivyStylePatterns)

addSbtPlugin("me.lessis" % "bintray-sbt" % "0.1.2")

addSbtPlugin("com.trueaccord.scalapb" % "sbt-scalapb" % "0.4.15")
