package io.lenses.streamreactor.connect

import _root_.io.circe.parser._
import _root_.io.lenses.streamreactor.connect.model.Order
import _root_.io.lenses.streamreactor.connect.testcontainers.S3Authentication
import cats.effect.IO
import cats.effect.Resource
import com.jayway.jsonpath.internal.Utils.unescape
import com.typesafe.scalalogging.LazyLogging
import org.apache.commons.io.IOUtils
import org.scalatest.EitherValues
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.s3.model._
import software.amazon.awssdk.services.s3.S3Client
import software.amazon.awssdk.services.s3.S3Configuration

import java.io.InputStream
import java.net.URI
import scala.jdk.CollectionConverters.ListHasAsScala
import scala.jdk.CollectionConverters.SeqHasAsJava

object S3Utils extends EitherValues with LazyLogging {

  def createBucket(s3Client: S3Client, bucketName: String): Resource[IO, CreateBucketResponse] =
    Resource.make {
      IO {
        s3Client.createBucket(
          CreateBucketRequest
            .builder()
            .bucket(bucketName)
            .createBucketConfiguration(
              CreateBucketConfiguration.builder()
                .locationConstraint(BucketLocationConstraint.EU_WEST_1)
                .build(),
            )
            .build(),
        )
      }.onError {
        ex: Throwable =>
          logger.error("Error creating bucket", ex)
          IO.unit
      }
    } { _ =>
      IO {
        emptyBucket(s3Client, bucketName)
        deleteBucket(s3Client, bucketName)
        ()
      }.recoverWith {
        case ex: Throwable =>
          logger.error("Error deleting bucket", ex)
          IO.unit
      }
    }

  private def deleteBucket(s3Client: S3Client, bucketName: String): DeleteBucketResponse =
    s3Client.deleteBucket(
      DeleteBucketRequest
        .builder()
        .bucket(bucketName)
        .build(),
    )

  private def emptyBucket(s3Client: S3Client, bucketName: String): Unit =
    s3Client
      .listObjectsV2Paginator(
        ListObjectsV2Request.builder().bucket(bucketName).build(),
      )
      .iterator()
      .forEachRemaining {
        o =>
          val deleteObjectsRequest = DeleteObjectsRequest
            .builder()
            .bucket(bucketName)
            .delete {
              val objects = o.contents().asScala.map(k => ObjectIdentifier.builder().key(k.key()).build())
              Delete
                .builder()
                .objects(objects.asJava).build()
            }
            .build()
          s3Client.deleteObjects(deleteObjectsRequest)
          ()
      }

  def createS3ClientResource(identity: S3Authentication, endpoint: URI): Resource[IO, S3Client] =
    Resource.fromAutoCloseable(
      IO(
        S3Client
          .builder()
          .endpointOverride(endpoint)
          .region(Region.of("custom"))
          .credentialsProvider(() => AwsBasicCredentials.create(identity.identity, identity.credential))
          .serviceConfiguration(S3Configuration
            .builder
            .pathStyleAccessEnabled(true)
            .build)
          .build(),
      ),
    )

  def readKeyToOrder(s3Client: S3Client, bucketName: String, key: String): Order = {
    val is: InputStream = s3Client.getObject(GetObjectRequest.builder().bucket(bucketName).key(key).build())
    val fileBytes = IOUtils.toByteArray(is)
    decodeEscapedJson(new String(fileBytes))
  }

  private def decodeEscapedJson(input: String): Order = {
    val a1 = unescape(input.trim)
    // remove the quotes from around the json
    val a2 = a1.substring(1, a1.length - 1)
    decode[Order](a2).value
  }

}
