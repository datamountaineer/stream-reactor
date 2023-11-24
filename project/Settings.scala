import Dependencies.globalExcludeDeps
import Dependencies.googleProtobuf
import Dependencies.googleProtobufJava
import Dependencies.hadoopCommon
import Dependencies.hadoopMapReduceClientCore
import Dependencies.nettyCodecSocks
import Dependencies.nettyHandlerProxy
import Dependencies.woodstoxCore
import com.eed3si9n.jarjarabrams.ShadeRule
import com.simplytyped.Antlr4Plugin
import com.simplytyped.Antlr4Plugin.autoImport.Antlr4
import com.simplytyped.Antlr4Plugin.autoImport.antlr4GenVisitor
import com.simplytyped.Antlr4Plugin.autoImport.antlr4PackageName
import com.simplytyped.Antlr4Plugin.autoImport.antlr4TreatWarningsAsErrors
import com.simplytyped.Antlr4Plugin.autoImport.antlr4Version
import de.heikoseeberger.sbtheader.HeaderPlugin.autoImport.*
import sbt.Keys.*
import sbt.Package.ManifestAttributes
import sbt.Compile
import sbt.Def
import sbt.*
import sbtassembly.AssemblyKeys.*
import sbtassembly.MergeStrategy
import sbtassembly.PathList

import java.nio.file.Files
import java.nio.file.Paths
import java.time.Year
import java.util.Calendar
import scala.collection.Seq
import scala.sys.process.*

object Settings extends Dependencies {

  // keep the SNAPSHOT version numerically higher than the latest release.
  val majorVersion        = "5.0"
  val nextSnapshotVersion = "5.1"

  val artifactVersion: String = {
    val maybeGithubRunId = sys.env.get("github_run_id")
    val maybeVersion     = sys.env.get("VERSION")
    val snapshotTag      = sys.env.get("SNAPSHOT_TAG")
    (maybeVersion, maybeGithubRunId) match {
      case (_, Some(patchVersion)) => majorVersion + "." + patchVersion
      case (Some(v), _)            => v
      case _                       => s"$nextSnapshotVersion-${snapshotTag.fold("SNAPSHOT")(t => s"$t-SNAPSHOT")}"
    }
  }

  val currentYear = Year.now().getValue()

  object ScalacFlags {
    val availableProcessors: String = java.lang.Runtime.getRuntime.availableProcessors.toString

    val commonOptions = Seq(
      // standard settings
      "-release:11",
      "-encoding",
      "UTF-8",
      "-unchecked",
      "-deprecation",
      "-explaintypes",
      "-feature",
      // language features
      "-language:existentials",
      "-language:higherKinds",
      "-language:implicitConversions",
      "-language:postfixOps",
      // private options
      "-Ybackend-parallelism",
      availableProcessors,
      "-Yrangepos", // required by SemanticDB compiler plugin
    )

    val lintings = List(
      "-Xlint:adapted-args",
      "-Xlint:constant",
      "-Xlint:delayedinit-select",
      "-Xlint:doc-detached",
      "-Xlint:inaccessible",
      "-Xlint:infer-any",
      "-Xlint:missing-interpolator",
      "-Xlint:nullary-unit",
      "-Xlint:option-implicit",
      "-Xlint:package-object-classes",
      "-Xlint:poly-implicit-overload",
      "-Xlint:private-shadow",
      "-Xlint:stars-align",
      "-Xlint:type-parameter-shadow",
    )

    object Scala213 {
      val WarnUnusedImports = "-Wunused:imports"
      val FatalWarnings     = "-Werror"
      val ValueDiscard      = "-Wvalue-discard"

      val warnings = List(
        FatalWarnings,
        ValueDiscard,
        WarnUnusedImports,
        "-Wdead-code",
        "-Wextra-implicit",
        "-Wmacros:after",
        "-Wnumeric-widen",
        "-Wunused:implicits",
        "-Wunused:locals",
        "-Wunused:patvars",
        "-Wunused:privates",
      )

      val options: Seq[String] = commonOptions ++ List(
        "-Xcheckinit",
      ) ++ warnings ++ lintings
    }
  }

  private val commonSettings: Seq[Setting[_]] = Seq(
    organization := "io.lenses",
    version := artifactVersion,
    scalaOrganization := Dependencies.scalaOrganization,
    scalaVersion := Dependencies.scalaVersion,
    headerEmptyLine := false,
    isSnapshot := artifactVersion.contains("SNAPSHOT"),
    javacOptions ++= Seq("--release", "11"),
    packageOptions := Seq(
      ManifestAttributes(
        ("Git-Commit-Hash", "git rev-parse HEAD".!!.trim),
        ("Git-Repo", "git config --get remote.origin.url".!!.trim),
        ("Git-Tag", sys.env.getOrElse("SNAPSHOT_TAG", "n/a")),
        ("Kafka-Version", KafkaVersionAxis.kafkaVersion),
        ("StreamReactor-Version", artifactVersion),
        ("StreamReactor-Docs", "https://docs.lenses.io/5.0/integrations/connectors/stream-reactor/"),
      ),
    ),
  )

  val settings: Seq[Setting[_]] = commonSettings ++ Seq(
    scalacOptions ++= ScalacFlags.Scala213.options,
    Compile / console / scalacOptions := ScalacFlags.commonOptions,
    Global / cancelable := true,
    Compile / fork := true,
    Compile / connectInput := true,
    Compile / outputStrategy := Some(StdoutOutput),
    resolvers ++= projectResolvers,
    scalaVersion := Dependencies.scalaVersion,
    excludeDependencies ++= globalExcludeDeps,
    headerLicense := Some(HeaderLicense.ALv2(s"2017-$currentYear", "Lenses.io Ltd")),
  )

  implicit final class ParallelDestroyer(project: Project) {
    def disableParallel(): Project =
      project.settings(settings ++ Seq(
        Test / parallelExecution := false,
        IntegrationTest / parallelExecution := false,
      ))
  }

  implicit final class AssemblyConfigurator(project: Project) {

    val excludeFilePatterns = Set(".MF", ".RSA", ".DSA", ".SF")

    def excludeFileFilter(p: String): Boolean =
      excludeFilePatterns.exists(p.endsWith)

    val excludePatterns = Set(
      "kafka-client",
      "kafka-connect-json",
      "hadoop-yarn",
      "org.apache.kafka",
      "zookeeper",
      "log4j",
      "junit",
    )

    def configureAssembly(overrideNetty: Boolean): Project = {
      val nettyDepOverrides = if (overrideNetty) nettyOverrides else Seq.empty
      project.settings(
        settings ++ Seq(
          assembly / assemblyOutputPath := {
            file(target.value + "/libs/").mkdirs()
            file(target.value + "/libs/" + (assembly / assemblyJarName).value)
          },
          assembly / assemblyExcludedJars := {
            val cp: Classpath = (assembly / fullClasspath).value
            cp filter { f =>
              excludePatterns.exists(f.data.getName.contains) && (!f.data.getName.contains("slf4j"))
            }
          },
          assembly / assemblyMergeStrategy := {
            case PathList("META-INF", "MANIFEST.MF")                 => MergeStrategy.discard
            case p if excludeFileFilter(p)                           => MergeStrategy.discard
            case PathList(ps @ _*) if ps.last == "module-info.class" => MergeStrategy.discard
            case _                                                   => MergeStrategy.first
          },
          assembly / assemblyShadeRules ++= Seq(
            ShadeRule.rename("org.apache.avro.**" -> "lshaded.apache.avro.@1").inAll,
            ShadeRule.rename("io.confluent.**" -> "lshaded.confluent.@1").inAll,
            ShadeRule.rename("com.fasterxml.**" -> "lshaded.fasterxml.@1").inAll,
            ShadeRule.rename("org.apache.hadoop" -> "lshaded.apache.hadoop").inAll,
          ),
          dependencyOverrides ++= Seq(
            googleProtobuf,
            googleProtobufJava,
            hadoopCommon,
            hadoopMapReduceClientCore,
            woodstoxCore,
          ) ++ nettyDepOverrides ++ avroOverrides,
        ),
      )
    }
  }

  implicit final class ProjectFilterOps(project: Project) {

    def containsDir(dir: String): Boolean = {
      val files = Files.find(
        Paths.get(project.base.getAbsolutePath),
        2,
        (p, _) => p.toFile.getAbsolutePath.endsWith(dir),
      ).toArray
      files.nonEmpty
    }
  }

  val IntegrationTest: Configuration = config("it").extend(Test).describedAs("Runs integration tests")
  val FunctionalTest:  Configuration = config("fun").extend(Test).describedAs("Runs system and acceptance tests")

  sealed abstract class TestConfigurator(
    project:         Project,
    config:          Configuration,
    defaultSettings: Seq[Def.Setting[_]] = Defaults.testSettings,
  ) {

    protected def configure(
      requiresFork: Boolean,
      testDeps:     Seq[ModuleID],
    ): Project =
      project
        .configs(config)
        .settings(
          libraryDependencies ++= testDeps.map(d => d % config),
          inConfig(config) {
            Defaults.testSettings ++
              Seq(
                config / unmanagedSourceDirectories ++= Seq(project.base / "src" / config.name / "scala"),
                config / unmanagedSourceDirectories ++= (Test / sourceDirectories).value,
                config / unmanagedResourceDirectories ++= (Test / resourceDirectories).value,
              )

            defaultSettings ++ Seq(
              fork := requiresFork,
              testFrameworks := Seq(sbt.TestFrameworks.ScalaTest),
              classLoaderLayeringStrategy := ClassLoaderLayeringStrategy.Flat,
              javaOptions += "-Dartifact.dir=" + (project.base / "target" / "libs").getAbsolutePath,
            ) ++ org.scalafmt.sbt.ScalafmtPlugin.scalafmtConfigSettings

          },
        )

  }

  implicit final class UnitTestConfigurator(project: Project) extends TestConfigurator(project, Test) {

    def configureTests(
      testDeps: Seq[ModuleID],
    ): Project =
      configure(requiresFork = false, testDeps)
  }

  implicit final class IntegrationTestConfigurator(project: Project)
      extends TestConfigurator(project, IntegrationTest) {

    def configureIntegrationTests(testDeps: Seq[ModuleID]): Project =
      configure(requiresFork = false, testDeps)
  }

  implicit final class FunctionalTestConfigurator(project: Project) extends TestConfigurator(project, FunctionalTest) {

    def configureFunctionalTests(extraDeps: Seq[ModuleID] = Seq.empty): Project = {
      val proj = configure(requiresFork = true, testCommonDeps ++ extraDeps)
      sys.env.get("CONNECT_IMAGE_VERSION").fold(proj)(vers =>
        proj.settings(
          FunctionalTest / envVars := Map(
            "CONFLUENT_VERSION" -> vers,
          ),
        ),
      )

    }

  }

  implicit final class AntlrConfigurator(project: Project) {

    def configureAntlr(): Project =
      project
        .enablePlugins(Antlr4Plugin)
        .settings(
          settings ++ Seq(
            Antlr4 / antlr4PackageName := Some("com.datamountaineer.kcql.antlr4"),
          ),
        )
  }

}
