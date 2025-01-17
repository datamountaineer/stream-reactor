import Dependencies._
import sbt._
import sbt.librarymanagement.InclExclRule

object Dependencies {

  val globalExcludeDeps: Seq[InclExclRule] = Seq(
    "org.jboss.logging"        % "*",
    "org.jboss.logmanager"     % "*",
    "org.jboss.sif4j"          % "*",
    "commons-logging"          % "commons-logging",
    "log4j"                    % "log4j",
    "org.slf4j"                % "slf4j-log4j12",
    "org.apache.logging.log4j" % "log4j",
    "org.apache.logging.log4j" % "log4j-core",
    "org.apache.logging.log4j" % "log4j-slf4j-impl",
    "com.sun.jersey"           % "*",
    "org.jline"                % "*",
    "org.codehaus.janino"      % "*",
  )

  // scala versions
  val scalaOrganization = "org.scala-lang"
  val scalaVersion      = "2.13.15"
  val supportedScalaVersions: Seq[String] = List(Dependencies.scalaVersion)

  val commonResolvers: Seq[MavenRepository] = Resolver.sonatypeOssRepos("public") ++
    Seq(
      Resolver typesafeRepo "releases",
      Resolver.mavenLocal,
      "confluent" at "https://packages.confluent.io/maven/",
      "typesafe" at "https://repo.typesafe.com/typesafe/releases/",
      "cloudera" at "https://repository.cloudera.com/artifactory/cloudera-repos/",
      "jitpack" at "https://jitpack.io",
    )

  object Versions {
    // libraries versions
    val scalatestVersion               = "3.2.19"
    val scalatestPlusScalaCheckVersion = "3.1.0.0-RC2"
    val scalaCheckVersion              = "1.18.1"

    val mockitoJunitJupiterVersion = "5.14.2"
    val junitJupiterVersion        = "5.11.4"
    val assertjCoreVersion         = "3.26.3"

    val cyclopsVersion = "10.4.1"

    val kafkaVersion:     String = "3.8.0"
    val confluentVersion: String = "7.7.2"

    val enumeratumVersion = "1.7.5"

    val http4sVersion    = "1.0.0-M32"
    val http4sJdkVersion = "1.0.0-M1"
    val avroVersion      = "1.12.0"
    val avro4sVersion    = "4.1.2"

    val catsEffectVersion     = "3.5.7"
    val `cats-effect-testing` = "1.5.0"

    val antlr4Version: String = "4.13.2"

    val circeVersion              = "0.15.0-M1"
    val circeGenericExtrasVersion = "0.14.4"

    // build plugins version
    val betterMonadicForVersion = "0.3.1"

    val lombokVersion = "1.18.36"

    val logbackVersion      = "1.5.11"
    val scalaLoggingVersion = "3.9.5"

    val dnsJavaVersion  = "3.6.2"
    val wiremockVersion = "3.10.0"
    val parquetVersion  = "1.14.4"

    val jerseyCommonVersion = "3.1.9"

    val calciteVersion = "1.34.0"
    val awsSdkVersion  = "2.29.23"

    val azureDataLakeVersion              = "12.22.0"
    val azureIdentityVersion              = "1.14.2"
    val azureCoreVersion                  = "1.54.1"
    val msal4jVersion                     = "1.17.3"
    val msal4jPersistenceExtensionVersion = "1.3.0"
    val gcpCloudVersion                   = "2.48.0"
    val gcpCloudStorageVersion            = "2.44.0"

    val woodstoxVersion     = "7.1.0"
    val jacksonVersion      = "2.18.2"
    val json4sVersion       = "4.0.7"
    val mockitoScalaVersion = "1.17.37"
    val mockitoJavaVersion  = "5.2.0"
    val openCsvVersion      = "5.9"
    val jsonSmartVersion    = "2.5.1"

    val xzVersion  = "1.10"
    val lz4Version = "1.8.0"

    val bouncyCastleVersion = "1.79"
    val nettyVersion        = "4.1.115.Final"

    val cassandraDriverVersion = "3.11.5"
    val jsonPathVersion        = "2.9.0"

    val azureDocumentDbVersion     = "2.6.5"
    val testcontainersScalaVersion = "0.41.4"
    val testcontainersVersion      = "1.20.1"

    val influxVersion = "7.2.0"

    val jmsApiVersion         = "3.1.0"
    val activeMqVersion       = "6.1.4"
    val protocVersion         = "3.11.4"
    val googleProtobufVersion = "3.25.5"

    val mqttVersion = "1.2.5"

    val commonsNetVersion      = "3.11.1"
    val commonsCodecVersion    = "1.17.1"
    val commonsCompressVersion = "1.27.1"
    val commonsConfigVersion   = "2.11.0"
    val commonsIOVersion       = "2.18.0"
    val commonsHttpVersion     = "4.5.14"
    val commonsLang3Version    = "3.17.0"
    val jschVersion            = "0.2.20"

    val minaVersion           = "2.2.3"
    val betterFilesVersion    = "3.9.2"
    val ftpServerVersion      = "1.2.0"
    val fakeSftpServerVersion = "2.0.0"

    val mongoDbVersion = "3.12.14"

    val jedisVersion = "5.1.5"
    val gsonVersion  = "2.11.0"

    val classGraphVersion           = "4.8.179"
    val nimbusJoseJwtVersion        = "9.47"
    val hadoopVersion               = "3.4.0"
    val hadoopShadedProtobufVersion = "1.2.0"

    val airCompressorVersion = "2.0.2"
    val zstdVersion          = "1.5.6-8"

    trait ElasticVersions {
      val elastic4sVersion, elasticSearchVersion, jnaVersion: String
    }

    object Elastic6Versions extends ElasticVersions() {
      override val elastic4sVersion:     String = "6.7.8"
      override val elasticSearchVersion: String = "6.8.23"
      override val jnaVersion:           String = "3.3.0"
    }

    object Elastic7Versions extends ElasticVersions {
      override val elastic4sVersion:     String = "7.17.4"
      override val elasticSearchVersion: String = "7.17.25"
      override val jnaVersion:           String = "5.15.0"
    }

  }

  import Versions._

  // functional libraries
  val catsEffectKernel = "org.typelevel" %% "cats-effect-kernel" % catsEffectVersion
  val catsEffectStd    = "org.typelevel" %% "cats-effect-std"    % catsEffectVersion
  val catsEffect       = "org.typelevel" %% "cats-effect"        % catsEffectVersion

  val circeGeneric       = "io.circe" %% "circe-generic"        % circeVersion
  val circeGenericExtras = "io.circe" %% "circe-generic-extras" % circeGenericExtrasVersion
  val circeParser        = "io.circe" %% "circe-parser"         % circeVersion
  val circe: Seq[ModuleID] = Seq(circeGeneric, circeParser, circeGenericExtras)

  val betterMonadicFor = addCompilerPlugin("com.olegpy" %% "better-monadic-for" % Versions.betterMonadicForVersion)

  // logging
  val logback          = "ch.qos.logback"              % "logback-classic"  % logbackVersion
  lazy val logbackCore = "ch.qos.logback"              % "logback-core"     % logbackVersion
  val scalaLogging     = "com.typesafe.scala-logging" %% "scala-logging"    % scalaLoggingVersion
  val log4jToSlf4j     = "org.slf4j"                   % "log4j-over-slf4j" % "2.0.16"
  val jclToSlf4j       = "org.slf4j"                   % "jcl-over-slf4j"   % "2.0.16"

  // testing
  val scalatest = "org.scalatest" %% "scalatest" % scalatestVersion
  val scalatestPlusScalaCheck =
    "org.scalatestplus" %% "scalatestplus-scalacheck" % scalatestPlusScalaCheckVersion
  val scalaCheck            = "org.scalacheck" %% "scalacheck"            % scalaCheckVersion
  val `mockitoScala`        = "org.mockito"    %% "mockito-scala"         % mockitoScalaVersion
  val `mockitoJava`         = "org.mockito"     % "mockito-inline"        % mockitoJavaVersion
  val `mockitoJunitJupiter` = "org.mockito"     % "mockito-junit-jupiter" % mockitoJunitJupiterVersion

  val `junitJupiter`       = "org.junit.jupiter" % "junit-jupiter-api"    % junitJupiterVersion
  val `junitJupiterParams` = "org.junit.jupiter" % "junit-jupiter-params" % junitJupiterVersion

  val `assertjCore` = "org.assertj" % "assertj-core" % assertjCoreVersion

  val `cyclops`     = "com.oath.cyclops" % "cyclops"      % cyclopsVersion
  val `cyclopsPure` = "com.oath.cyclops" % "cyclops-pure" % cyclopsVersion

  val catsEffectScalatest = "org.typelevel" %% "cats-effect-testing-scalatest" % `cats-effect-testing`

  val enumeratumCore  = "com.beachape" %% "enumeratum"       % enumeratumVersion
  val enumeratumCirce = "com.beachape" %% "enumeratum-circe" % enumeratumVersion
  val enumeratum: Seq[ModuleID] = Seq(enumeratumCore, enumeratumCirce)

  val kafkaConnectJson: ModuleID = "org.apache.kafka" % "connect-json"  % kafkaVersion
  val kafkaClients:     ModuleID = "org.apache.kafka" % "kafka-clients" % kafkaVersion

  val confluentJsonSchemaSerializer: ModuleID =
    "io.confluent" % "kafka-json-schema-serializer" % confluentVersion

  def confluentExcludes(moduleID: ModuleID): ModuleID = moduleID
    .exclude("org.apache.kafka", "kafka-clients")
    .exclude("javax.ws.rs", "javax.ws.rs-api")
    .excludeAll(ExclusionRule(organization = "io.swagger"))
    .excludeAll(ExclusionRule(organization = "com.fasterxml.jackson.core"))
    .excludeAll(ExclusionRule(organization = "com.fasterxml.jackson"))
    .excludeAll(ExclusionRule(organization = "com.fasterxml.jackson.databind"))

  val confluentAvroConverter: ModuleID =
    confluentExcludes("io.confluent" % "kafka-connect-avro-converter" % confluentVersion)

  val confluentAvroData: ModuleID =
    confluentExcludes("io.confluent" % "kafka-connect-avro-data" % confluentVersion)

  val confluentProtobufConverter: ModuleID =
    confluentExcludes("io.confluent" % "kafka-connect-protobuf-converter" % confluentVersion)

  val http4sDsl       = "org.http4s" %% "http4s-dsl"             % http4sVersion
  val http4sJdkClient = "org.http4s" %% "http4s-jdk-http-client" % http4sJdkVersion
  val http4sCirce     = "org.http4s" %% "http4s-circe"           % http4sVersion

  val histogram = "org.hdrhistogram" % "HdrHistogram" % "2.2.2" // Or latest version

  val http4s: Seq[ModuleID] = Seq(http4sDsl, http4sJdkClient, http4sCirce)

  val bouncyProv = "org.bouncycastle" % "bcprov-jdk18on" % bouncyCastleVersion
  val bouncyUtil = "org.bouncycastle" % "bcutil-jdk18on" % bouncyCastleVersion
  val bouncyPkix = "org.bouncycastle" % "bcpkix-jdk18on" % bouncyCastleVersion
  val bouncyBcpg = "org.bouncycastle" % "bcpg-jdk18on"   % bouncyCastleVersion
  val bouncyTls  = "org.bouncycastle" % "bctls-jdk18on"  % bouncyCastleVersion
  val bouncyCastle: Seq[ModuleID] = Seq(bouncyProv, bouncyUtil, bouncyPkix, bouncyBcpg, bouncyTls)

  lazy val avro         = "org.apache.avro"      % "avro"          % avroVersion
  lazy val avroProtobuf = "org.apache.avro"      % "avro-protobuf" % avroVersion
  lazy val avro4s       = "com.sksamuel.avro4s" %% "avro4s-core"   % avro4sVersion
  lazy val avro4sJson   = "com.sksamuel.avro4s" %% "avro4s-json"   % avro4sVersion

  val `wiremock` = "org.wiremock" % "wiremock" % wiremockVersion

  val jerseyCommon = "org.glassfish.jersey.core" % "jersey-common" % jerseyCommonVersion

  lazy val parquetAvro:   ModuleID = "org.apache.parquet" % "parquet-avro"   % parquetVersion
  lazy val parquetHadoop: ModuleID = "org.apache.parquet" % "parquet-hadoop" % parquetVersion

  lazy val dnsJava: ModuleID = "dnsjava" % "dnsjava" % dnsJavaVersion
  lazy val hadoopCommon: ModuleID = hiveExcludes("org.apache.hadoop" % "hadoop-common" % hadoopVersion)
    .excludeAll(ExclusionRule(organization = "javax.servlet"))
    .excludeAll(ExclusionRule(organization = "javax.servlet.jsp"))
    .excludeAll(ExclusionRule(organization = "org.mortbay.jetty"))
    .excludeAll(ExclusionRule(organization = "org.codehaus.jackson"))
    .excludeAll(ExclusionRule(organization = "com.fasterxml.jackson"))
    .excludeAll(ExclusionRule(organization = "com.fasterxml.jackson.core"))
    .excludeAll(ExclusionRule(organization = "com.fasterxml.jackson.databind"))
    .exclude("org.apache.hadoop", "hadoop-annotations")
    .exclude("org.apache.hadoop", "hadoop-auth")
    .excludeAll(ExclusionRule(organization = "org.eclipse.jetty"))

  lazy val hadoopMapReduce: ModuleID = hiveExcludes("org.apache.hadoop" % "hadoop-mapreduce" % hadoopVersion)
  lazy val hadoopMapReduceClient: ModuleID =
    hiveExcludes("org.apache.hadoop" % "hadoop-mapreduce-client" % hadoopVersion)
  lazy val hadoopMapReduceClientCore: ModuleID =
    hiveExcludes("org.apache.hadoop" % "hadoop-mapreduce-client-core" % hadoopVersion)
  lazy val hadoopShadedProtobuf: ModuleID =
    hiveExcludes("org.apache.hadoop.thirdparty" % "hadoop-shaded-protobuf_3_21" % hadoopShadedProtobufVersion)

  lazy val calciteCore = hiveExcludes("org.apache.calcite" % "calcite-core" % calciteVersion)
    .excludeAll(ExclusionRule(organization = "io.swagger"))
    .excludeAll(ExclusionRule(organization = "io.netty"))
    .excludeAll(ExclusionRule(organization = "com.fasterxml.jackson.core"))
    .excludeAll(ExclusionRule(organization = "com.fasterxml.jackson"))
    .excludeAll(ExclusionRule(organization = "com.fasterxml.jackson.databind"))
    .excludeAll(ExclusionRule(organization = "org.apache.zookeeper"))
    .excludeAll(ExclusionRule(organization = "org.eclipse.jetty"))

  lazy val calciteLinq4J = "org.apache.calcite" % "calcite-linq4j" % calciteVersion

  lazy val lombok = "org.projectlombok" % "lombok" % lombokVersion

  lazy val s3Sdk  = "software.amazon.awssdk" % "s3"  % awsSdkVersion
  lazy val stsSdk = "software.amazon.awssdk" % "sts" % awsSdkVersion

  lazy val azureDataLakeSdk: ModuleID = "com.azure" % "azure-storage-file-datalake" % azureDataLakeVersion
  lazy val azureIdentity:    ModuleID = "com.azure" % "azure-identity"              % azureIdentityVersion
  lazy val azureCore:        ModuleID = "com.azure" % "azure-core"                  % azureCoreVersion
  lazy val msal4j: ModuleID =
    "com.microsoft.azure" % "msal4j" % msal4jVersion
  lazy val msal4jPersistenceExtension: ModuleID =
    "com.microsoft.azure" % "msal4j-persistence-extension" % msal4jPersistenceExtensionVersion

  lazy val gcpCloudCoreSdk = "com.google.cloud" % "google-cloud-core"      % gcpCloudVersion
  lazy val gcpCloudHttp    = "com.google.cloud" % "google-cloud-core-http" % gcpCloudVersion
  lazy val gcpStorageSdk   = "com.google.cloud" % "google-cloud-storage"   % gcpCloudStorageVersion

  lazy val json4sNative  = "org.json4s" %% "json4s-native"  % json4sVersion
  lazy val json4sJackson = "org.json4s" %% "json4s-jackson" % json4sVersion
  val jacksonCore: ModuleID = "com.fasterxml.jackson.core" % "jackson-core" % jacksonVersion
  val jacksonDatabind: ModuleID =
    "com.fasterxml.jackson.core" % "jackson-databind" % jacksonVersion
  val jacksonDataformatCbor: ModuleID =
    "com.fasterxml.jackson.dataformat" % "jackson-dataformat-cbor" % jacksonVersion
  val jacksonModuleScala: ModuleID =
    "com.fasterxml.jackson.module" %% "jackson-module-scala" % jacksonVersion
  val woodstoxCore: ModuleID =
    "com.fasterxml.woodstox" % "woodstox-core" % woodstoxVersion
  val jsonSmart: ModuleID =
    "net.minidev" % "json-smart" % jsonSmartVersion

  lazy val openCsv = "com.opencsv" % "opencsv" % openCsvVersion

  lazy val cassandraDriver = "com.datastax.cassandra" % "cassandra-driver-core" % cassandraDriverVersion
  lazy val jsonPath        = "com.jayway.jsonpath"    % "json-path"             % jsonPathVersion

  lazy val nettyCommon       = "io.netty" % "netty-common"                 % nettyVersion
  lazy val nettyHandler      = "io.netty" % "netty-handler"                % nettyVersion
  lazy val nettyHandlerProxy = "io.netty" % "netty-handler-proxy"          % nettyVersion
  lazy val nettyCodec        = "io.netty" % "netty-codec"                  % nettyVersion
  lazy val nettyCodecHttp    = "io.netty" % "netty-codec-http"             % nettyVersion
  lazy val nettyCodecSocks   = "io.netty" % "netty-codec-socks"            % nettyVersion
  lazy val nettyResolver     = "io.netty" % "netty-resolver"               % nettyVersion
  lazy val nettyTransport    = "io.netty" % "netty-transport-native-epoll" % nettyVersion classifier "linux-x86_64"

  lazy val azureDocumentDb = "com.microsoft.azure" % "azure-documentdb" % azureDocumentDbVersion

  // testcontainers
  lazy val testContainersScala = "com.dimafeng" %% "testcontainers-scala-scalatest" % testcontainersScalaVersion
  lazy val testContainersScalaCassandra =
    "com.dimafeng" %% "testcontainers-scala-cassandra" % testcontainersScalaVersion
  lazy val testContainersScalaMongodb = "com.dimafeng" %% "testcontainers-scala-mongodb" % testcontainersScalaVersion
  lazy val testContainersScalaToxiProxy =
    "com.dimafeng" %% "testcontainers-scala-toxiproxy" % testcontainersScalaVersion
  lazy val testContainersScalaElasticsearch =
    "com.dimafeng" %% "testcontainers-scala-elasticsearch" % testcontainersScalaVersion

  lazy val testcontainersCore          = "org.testcontainers" % "testcontainers" % testcontainersVersion
  lazy val testcontainersKafka         = "org.testcontainers" % "kafka"          % testcontainersVersion
  lazy val testcontainersCassandra     = "org.testcontainers" % "cassandra"      % testcontainersVersion
  lazy val testcontainersElasticsearch = "org.testcontainers" % "elasticsearch"  % testcontainersVersion
  lazy val testcontainersMongodb       = "org.testcontainers" % "mongodb"        % testcontainersVersion

  lazy val influx = "com.influxdb" % "influxdb-client-java" % influxVersion

  lazy val jmsApi         = "jakarta.jms"         % "jakarta.jms-api" % jmsApiVersion
  lazy val activeMq       = "org.apache.activemq" % "activemq-client" % activeMqVersion
  lazy val activeMqBroker = "org.apache.activemq" % "activemq-broker" % activeMqVersion

  lazy val protoc             = "com.github.os72"     % "protoc-jar"    % protocVersion
  lazy val googleProtobufJava = "com.google.protobuf" % "protobuf-java" % googleProtobufVersion
  lazy val googleProtobuf     = "com.google.protobuf" % "protobuf-java" % googleProtobufVersion % "protobuf"

  lazy val mqttClient = "org.eclipse.paho" % "org.eclipse.paho.client.mqttv3" % mqttVersion

  lazy val commonsNet      = "commons-net"               % "commons-net"             % commonsNetVersion
  lazy val commonsCodec    = "commons-codec"             % "commons-codec"           % commonsCodecVersion
  lazy val commonsIO       = "commons-io"                % "commons-io"              % commonsIOVersion
  lazy val commonsLang3    = "org.apache.commons"        % "commons-lang3"           % commonsLang3Version
  lazy val commonsCompress = "org.apache.commons"        % "commons-compress"        % commonsCompressVersion
  lazy val commonsConfig   = "org.apache.commons"        % "commons-configuration2"  % commonsConfigVersion
  lazy val commonsHttp     = "org.apache.httpcomponents" % "httpclient"              % commonsHttpVersion
  lazy val jsch            = "com.github.mwiede"         % "jsch"                    % jschVersion
  lazy val mina            = "org.apache.mina"           % "mina-core"               % minaVersion
  lazy val betterFiles     = "com.github.pathikrit"     %% "better-files"            % betterFilesVersion
  lazy val ftpServer       = "org.apache.ftpserver"      % "ftpserver-core"          % ftpServerVersion
  lazy val fakeSftpServer  = "com.github.stefanbirkner"  % "fake-sftp-server-lambda" % fakeSftpServerVersion

  lazy val mongoDb = "org.mongodb" % "mongo-java-driver" % mongoDbVersion

  lazy val jedis = "redis.clients"        % "jedis" % jedisVersion
  lazy val gson  = "com.google.code.gson" % "gson"  % gsonVersion

  lazy val xz  = "org.tukaani" % "xz"       % xzVersion
  lazy val lz4 = "org.lz4"     % "lz4-java" % lz4Version

  def hiveExcludes(moduleID: ModuleID): ModuleID =
    moduleID
      .excludeAll(ExclusionRule(organization = "org.mortbay.jetty"))
      .excludeAll(ExclusionRule(organization = "org.eclipse.jetty"))
      .excludeAll(ExclusionRule(organization = "org.apache.calcite"))
      .excludeAll(ExclusionRule(organization = "org.apache.parquet"))
      .excludeAll(ExclusionRule(organization = "org.apache.calcite.avatica"))
      .excludeAll(ExclusionRule(organization = "org.apache.hadoop"))
      .excludeAll(ExclusionRule(organization = "io.netty"))
      .excludeAll(ExclusionRule(organization = "com.nibusds"))
      .exclude("com.fasterxml.jackson.core", "jackson-annotations")

  lazy val classGraph    = "io.github.classgraph" % "classgraph"    % classGraphVersion
  lazy val nimbusJoseJwt = hiveExcludes("com.nimbusds" % "nimbus-jose-jwt" % nimbusJoseJwtVersion)
  lazy val airCompressor = "io.airlift"           % "aircompressor" % airCompressorVersion
  lazy val zstd          = "com.github.luben"     % "zstd-jni"      % zstdVersion

  // testcontainers module only
  lazy val festAssert = "org.easytesting" % "fest-assert" % "1.4"

  def elastic4sCore(v:    String): ModuleID = "com.sksamuel.elastic4s" %% "elastic4s-core"          % v
  def elastic4sClient(v:  String): ModuleID = "com.sksamuel.elastic4s" %% "elastic4s-client-esjava" % v
  def elastic4sTestKit(v: String): ModuleID = "com.sksamuel.elastic4s" %% "elastic4s-testkit"       % v
  def elastic4sHttp(v:    String): ModuleID = "com.sksamuel.elastic4s" %% "elastic4s-http"          % v

  def elasticSearch(v:         String): ModuleID = "org.elasticsearch"                 % "elasticsearch"   % v
  def elasticSearchAnalysis(v: String): ModuleID = "org.codelibs.elasticsearch.module" % "analysis-common" % v

  def jna(v: String): ModuleID = "net.java.dev.jna" % "jna" % v

}

trait Dependencies {

  import Versions._

  val loggingDeps: Seq[ModuleID] = Seq(
    "org.apache.logging.log4j" % "log4j-api"      % "2.24.2",
    "org.apache.logging.log4j" % "log4j-to-slf4j" % "2.24.2",
    log4jToSlf4j,
    jclToSlf4j,
    logback,
    logbackCore,
    scalaLogging,
  )

  // resolvers
  val projectResolvers: Seq[MavenRepository] = commonResolvers

  val baseTestDeps: Seq[ModuleID] = loggingDeps ++ Seq(
    catsEffectKernel,
    catsEffectStd,
    catsEffect,
    scalatest,
    catsEffectScalatest,
    scalatestPlusScalaCheck,
    scalaCheck,
    `mockitoScala`,
    `wiremock`,
    jerseyCommon,
    avro4s,
    kafkaClients,
  ) ++ enumeratum ++ circe ++ http4s ++ bouncyCastle

  //Specific modules dependencies
  val sqlCommonDeps: Seq[ModuleID] = loggingDeps ++ Seq(
    catsEffectKernel,
    catsEffectStd,
    catsEffect,
    calciteCore,
    calciteLinq4J,
    kafkaConnectJson,
    json4sNative,
    json4sJackson,
    jacksonCore,
    jacksonDatabind,
    jacksonModuleScala,
    jacksonDataformatCbor,
  ) ++ enumeratum ++ circe ++ http4s

  //Specific modules dependencies
  val baseDeps: Seq[ModuleID] = loggingDeps ++ Seq(
    jacksonDatabind,
    commonsCompress,
    avro4s,
    catsEffectKernel,
    catsEffect,
    kafkaConnectJson,
    confluentAvroConverter,
    confluentAvroData,
    confluentJsonSchemaSerializer,
  ) ++ enumeratum ++ circe

  val javaCommonDeps: Seq[ModuleID] = Seq(lombok, kafkaConnectJson, kafkaClients, cyclops, `cyclopsPure`)
  val javaCommonTestDeps: Seq[ModuleID] =
    Seq(junitJupiter, junitJupiterParams, assertjCore, `mockitoJava`, mockitoJunitJupiter, logback) ++ bouncyCastle

  //Specific modules dependencies

  val kafkaConnectCloudCommonDeps: Seq[ModuleID] = Seq(
    parquetAvro,
    parquetHadoop,
    hadoopCommon,
    dnsJava,
    hadoopMapReduce,
    hadoopMapReduceClient,
    hadoopMapReduceClientCore,
    hadoopShadedProtobuf,
    commonsConfig,
    openCsv,
    jacksonCore,
    jacksonDatabind,
    jacksonModuleScala,
    jacksonDataformatCbor,
    airCompressor,
  )

  val kafkaConnectS3Deps: Seq[ModuleID] = Seq(
    commonsIO,
    s3Sdk,
    stsSdk,
  )

  val compressionCodecDeps: Seq[ModuleID] = Seq(xz, lz4)

  val kafkaConnectAzureDatalakeDeps: Seq[ModuleID] = Seq(
    azureDataLakeSdk,
    azureIdentity,
    azureCore,
  )

  val kafkaConnectGcpCommonDeps: Seq[ModuleID] = Seq(
    cyclops,
    lombok,
    kafkaClients,
    gcpCloudCoreSdk,
    gcpCloudHttp,
  )

  val kafkaConnectGcpStorageDeps: Seq[ModuleID] = Seq(
    cyclops,
    gcpStorageSdk,
  )

  val kafkaConnectGcpStorageTestDeps: Seq[ModuleID] =
    baseTestDeps ++ kafkaConnectGcpStorageDeps ++ compressionCodecDeps :+ testcontainersCore

  val kafkaConnectS3TestDeps: Seq[ModuleID] = baseTestDeps ++ compressionCodecDeps :+ testcontainersCore

  val kafkaConnectS3FuncTestDeps: Seq[ModuleID] = baseTestDeps ++ compressionCodecDeps :+ s3Sdk

  val kafkaConnectHttpDeps: Seq[ModuleID] = Seq(http4sJdkClient, http4sCirce, histogram)

  val kafkaConnectHttpTestDeps: Seq[ModuleID] = baseTestDeps ++ Seq(catsEffectScalatest)

  val kafkaConnectCassandraDeps: Seq[ModuleID] = Seq(
    cassandraDriver,
    jsonPath,
    nettyTransport,
  )

  val kafkaConnectCassandraTestDeps: Seq[ModuleID] =
    baseTestDeps ++ Seq(testContainersScala, testContainersScalaCassandra)

  val kafkaConnectAzureDocumentDbDeps: Seq[ModuleID] = Seq(azureDocumentDb)

  val kafkaConnectInfluxDbDeps: Seq[ModuleID] = Seq(influx, avro4s, avro4sJson)

  val kafkaConnectJmsDeps: Seq[ModuleID] = Seq(
    jmsApi,
    confluentProtobufConverter,
    protoc,
    googleProtobuf,
    googleProtobufJava,
    gson,
  )

  val kafkaConnectJmsTestDeps: Seq[ModuleID] = baseTestDeps ++ Seq(
    activeMq,
    activeMqBroker,
  )

  val kafkaConnectMqttDeps: Seq[ModuleID] = Seq(mqttClient, avro4s, avro4sJson) ++ bouncyCastle

  val kafkaConnectMqttTestDeps: Seq[ModuleID] = baseTestDeps ++ Seq(testContainersScala, testContainersScalaToxiProxy)

  def elasticCommonDeps(v: ElasticVersions): Seq[ModuleID] = Seq(
    elastic4sCore(v.elastic4sVersion),
    jna(v.jnaVersion),
    elasticSearch(v.elasticSearchVersion),
    commonsHttp,
    //elasticSearchAnalysis(v.elasticSearchVersion)
  )

  def elasticTestCommonDeps(v: ElasticVersions): Seq[ModuleID] = Seq(
    elastic4sTestKit(v.elastic4sVersion),
    testContainersScala,
    testContainersScalaElasticsearch,
  )

  val kafkaConnectElastic6Deps: Seq[ModuleID] =
    elasticCommonDeps(Elastic6Versions) ++ Seq(elastic4sHttp(Elastic6Versions.elastic4sVersion))

  val kafkaConnectElastic6TestDeps: Seq[ModuleID] = baseTestDeps ++ elasticTestCommonDeps(Elastic6Versions)

  val kafkaConnectElastic7Deps: Seq[ModuleID] =
    elasticCommonDeps(Elastic7Versions) ++ Seq(elastic4sClient(Elastic7Versions.elastic4sVersion))

  val kafkaConnectElastic7TestDeps: Seq[ModuleID] = baseTestDeps ++ elasticTestCommonDeps(Elastic7Versions)

  val kafkaConnectFtpDeps: Seq[ModuleID] = Seq(commonsNet, commonsCodec, commonsIO, commonsLang3, jsch)

  val kafkaConnectFtpTestDeps: Seq[ModuleID] = baseTestDeps ++ Seq(mina, betterFiles, ftpServer, fakeSftpServer)

  val kafkaConnectMongoDbDeps: Seq[ModuleID] = Seq(json4sJackson, json4sNative, mongoDb)

  val kafkaConnectMongoDbTestDeps: Seq[ModuleID] = baseTestDeps ++ Seq(testContainersScalaMongodb)

  val kafkaConnectRedisDeps: Seq[ModuleID] = Seq(jedis)

  val kafkaConnectRedisTestDeps: Seq[ModuleID] = (baseTestDeps ++ Seq(testContainersScala, gson))
    .map {
      moduleId: ModuleID => moduleId.extra("scope" -> "test")
    }

  val testCommonDeps: Seq[ModuleID] = baseDeps ++ Seq(
    scalatest,
    json4sJackson,
    json4sNative,
    testcontainersCore,
    testcontainersKafka,
    testcontainersCassandra,
    testcontainersElasticsearch,
    testcontainersMongodb,
    jedis,
    mongoDb,
  )

  val nettyOverrides: Seq[ModuleID] = Seq(
    nettyCommon,
    nettyHandler,
    nettyHandlerProxy,
    nettyCodec,
    nettyCodecHttp,
    nettyCodecSocks,
    nettyResolver,
    nettyTransport,
  )

  val avroOverrides: Seq[ModuleID] = Seq(
    avro,
    avroProtobuf,
  )

}
