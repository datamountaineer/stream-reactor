package io.lenses.streamreactor.connect

import _root_.io.lenses.streamreactor.connect.testcontainers.S3Authentication
import _root_.io.lenses.streamreactor.connect.testcontainers.connect._

object Configuration {

  def sinkConfig(
    randomTestId:     String,
    auth:             S3Authentication,
    networkAliasUrl:  String,
    bucketName:       String,
    prefix:           String,
    storeAs:          String         = "json",
    compressionCodec: Option[String] = Option.empty,
    compressionLevel: Option[Int]    = Option.empty,
    topicName:        String,
  ): ConnectorConfiguration =
    ConnectorConfiguration(
      "connector" + randomTestId,
      Map(
        "connector.class"            -> ConfigValue("io.lenses.streamreactor.connect.aws.s3.sink.S3SinkConnector"),
        "tasks.max"                  -> ConfigValue(1),
        "topics"                     -> ConfigValue(topicName),
        "connect.s3.aws.access.key"  -> ConfigValue(auth.identity),
        "connect.s3.aws.secret.key"  -> ConfigValue(auth.credential),
        "connect.s3.aws.auth.mode"   -> ConfigValue("Credentials"),
        "connect.s3.custom.endpoint" -> ConfigValue(networkAliasUrl),
        "connect.s3.vhost.bucket"    -> ConfigValue(true),
        "connect.s3.aws.region"      -> ConfigValue("eu-west-1"),
        "connect.s3.kcql" -> ConfigValue(
          s"INSERT INTO `$bucketName:$prefix` SELECT * FROM `$topicName` STOREAS `$storeAs` WITH_FLUSH_COUNT=1",
        ),
      ) ++ Seq(
        compressionCodec.map { s: String => ("connect.s3.compression.codec", ConfigValue(s)) },
        compressionLevel.map { i: Int =>
          ("connect.s3.compression.level", ConfigValue(Integer.valueOf(i)))
        },
      ).flatten.toMap,
    )

  def sourceConfig(
    randomTestId:     String,
    auth:             S3Authentication,
    networkAliasUrl:  String,
    bucketName:       String,
    prefix:           String,
    storeAs:          String         = "json",
    compressionCodec: Option[String] = Option.empty,
    compressionLevel: Option[Int]    = Option.empty,
    topicName:        String,
  ): ConnectorConfiguration =
    ConnectorConfiguration(
      "connector" + randomTestId,
      Map(
        "connector.class"            -> ConfigValue("io.lenses.streamreactor.connect.aws.s3.source.S3SourceConnector"),
        "tasks.max"                  -> ConfigValue(1),
        "topics"                     -> ConfigValue(topicName),
        "connect.s3.aws.access.key"  -> ConfigValue(auth.identity),
        "connect.s3.aws.secret.key"  -> ConfigValue(auth.credential),
        "connect.s3.aws.auth.mode"   -> ConfigValue("Credentials"),
        "connect.s3.custom.endpoint" -> ConfigValue(networkAliasUrl),
        "connect.s3.vhost.bucket"    -> ConfigValue(true),
        "connect.s3.aws.region"      -> ConfigValue("eu-west-1"),
        "connect.s3.kcql" -> ConfigValue(
          s"INSERT INTO `$topicName` SELECT * FROM `$bucketName:$prefix` STOREAS `$storeAs` WITH_FLUSH_COUNT=1",
        ),
        "connect.s3.partition.search.recurse.levels" -> ConfigValue(0),
      ) ++ Seq(
        compressionCodec.map { s: String => ("connect.s3.compression.codec", ConfigValue(s)) },
        compressionLevel.map { i: Int =>
          ("connect.s3.compression.level", ConfigValue(Integer.valueOf(i)))
        },
      ).flatten.toMap,
    )
}
