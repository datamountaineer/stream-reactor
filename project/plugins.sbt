// Activate the following only when needed to use specific tasks like `whatDependsOn` etc...
//addDependencyTreePlugin
ThisBuild / libraryDependencySchemes ++= Seq(
  "org.scala-lang.modules" %% "scala-xml" % VersionScheme.Always,
)
addSbtPlugin("org.scalameta"     % "sbt-scalafmt"       % "2.5.2")
addSbtPlugin("org.scoverage"     % "sbt-scoverage"      % "2.0.8")
addSbtPlugin("de.heikoseeberger" % "sbt-header"         % "5.9.0")
addSbtPlugin("com.typesafe.sbt"  % "sbt-license-report" % "1.2.0")

addSbtPlugin("net.vonbuchholtz" % "sbt-dependency-check" % "4.0.0")

addSbtPlugin("org.xerial.sbt" % "sbt-pack" % "0.14")

addCompilerPlugin("com.olegpy" %% "better-monadic-for" % "0.3.1")

addSbtPlugin("com.typesafe.sbt" % "sbt-license-report" % "1.2.0")

//addCompilerPlugin("org.typelevel" %% "kind-projector" % "0.13.2")

addSbtPlugin("com.simplytyped" % "sbt-antlr4" % "0.8.3")
