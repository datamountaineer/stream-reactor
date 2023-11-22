package io.lenses.streamreactor.connect

import cats.effect.IO
import com.typesafe.scalalogging.LazyLogging
import software.amazon.awssdk.services.s3.S3Client
import software.amazon.awssdk.services.s3.model._

import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths
import scala.jdk.CollectionConverters._

object ResourceCopier extends LazyLogging {

  def copyResources(
    bucketName: String,
    client:     S3Client,
    firstFile:  String,
    s3Path:     String,
  ): IO[List[PutObjectResponse]] =
    for {
      path  <- IO(Paths.get(getClass.getResource(firstFile).getPath).getParent)
      files <- IO(Files.list(path).iterator().asScala.toList)
    } yield {
      files.map {
        file: Path =>
          val fileLength = file.toFile.length()
          logger.info(s"Copying resource $file (length: $fileLength)")
          client.putObject(
            PutObjectRequest
              .builder()
              .bucket(bucketName)
              .key(s3Path + file.getFileName.toString)
              .contentLength(fileLength)
              .build(),
            file,
          )
      }
    }

}
