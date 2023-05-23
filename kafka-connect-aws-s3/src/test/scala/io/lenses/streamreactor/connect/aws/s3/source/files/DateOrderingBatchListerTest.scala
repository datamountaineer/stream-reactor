/*
 * Copyright 2017-2023 Lenses.io Ltd
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.lenses.streamreactor.connect.aws.s3.source.files

import cats.implicits._
import io.lenses.streamreactor.connect.aws.s3.model.location.RemoteS3PathLocation
import io.lenses.streamreactor.connect.aws.s3.model.location.RemoteS3RootLocation
import io.lenses.streamreactor.connect.aws.s3.storage.FileListError
import io.lenses.streamreactor.connect.aws.s3.storage.FileMetadata
import io.lenses.streamreactor.connect.aws.s3.storage.ListResponse
import io.lenses.streamreactor.connect.aws.s3.storage.StorageInterface
import org.mockito.ArgumentMatchersSugar.any
import org.mockito.ArgumentMatchersSugar._
import org.mockito.MockitoSugar
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.BeforeAndAfter
import org.scalatest.EitherValues
import org.scalatest.OptionValues
import software.amazon.awssdk.services.s3.model.S3Object

import java.time.Instant
import java.time.temporal.ChronoUnit

class DateOrderingBatchListerTest
    extends AnyFlatSpec
    with Matchers
    with MockitoSugar
    with BeforeAndAfter
    with OptionValues
    with EitherValues {

  private val pathLocation     = RemoteS3RootLocation("bucket:path").toPath()
  private val storageInterface = mock[StorageInterface]

  private val listerFn = DateOrderingBatchLister.listBatch(storageInterface, pathLocation, 10) _
  private val instBase = Instant.now
  private def instant(no: Int): Instant = {
    require(no <= 100)
    instBase.minus((100 - no).toLong, ChronoUnit.DAYS)
  }
  private val allFiles = (1 to 100).map(i => FileMetadata(s"/file$i", instant(i)))

  "listBatch" should "return first result when no TopicPartitionOffset has been provided" in {
    val returnFiles = allFiles.slice(0, 5)
    val storageInterfaceResponse: ListResponse[FileMetadata] = ListResponse[FileMetadata](
      pathLocation,
      returnFiles,
      returnFiles.last,
    )

    mockStorageInterface(storageInterfaceResponse)

    listerFn(none).value.value should be(ListResponse[String](
      pathLocation,
      returnFiles.map(_.file),
      returnFiles.last,
    ))
  }

  "listBatch" should "return empty when no results are found" in {

    when(
      storageInterface.listRecursive(
        eqTo(pathLocation),
        any[(RemoteS3PathLocation, Seq[S3Object]) => Option[ListResponse[FileMetadata]]],
      ),
    ).thenReturn(none.asRight)

    listerFn(none) should be(none.asRight)
  }

  "listBatch" should "pass through any errors" in {
    val exception = FileListError(new IllegalStateException("BadThingsHappened"), "Oh No")

    when(
      storageInterface.listRecursive(
        eqTo(pathLocation),
        any[(RemoteS3PathLocation, Seq[S3Object]) => Option[ListResponse[FileMetadata]]],
      ),
    ).thenReturn(
      exception.asLeft,
    )
    listerFn(none).left.value should be(exception)

  }

  "listBatch" should "sort allFiles in order" in {
    val returnFiles = allFiles.slice(0, 5)
    val storageInterfaceResponse: ListResponse[FileMetadata] = ListResponse[FileMetadata](
      pathLocation,
      returnFiles.reverse,
      returnFiles.head,
    )

    mockStorageInterface(storageInterfaceResponse)

    listerFn(none).value.value should be(ListResponse[String](
      pathLocation,
      returnFiles.map(_.file),
      returnFiles.last,
    ))
  }

  "listBatch" should "return requested number of results in order" in {
    val storageInterfaceResponse: ListResponse[FileMetadata] = ListResponse[FileMetadata](
      pathLocation,
      allFiles.reverse,
      allFiles.head,
    )

    mockStorageInterface(storageInterfaceResponse)

    listerFn(none).value.value should be(ListResponse[String](
      pathLocation,
      allFiles.take(10).map(_.file),
      allFiles(9),
    ))
  }

  "listBatch" should "resume from the next file in date order" in {
    val storageInterfaceResponse: ListResponse[FileMetadata] = ListResponse[FileMetadata](
      pathLocation,
      allFiles.reverse,
      allFiles.head,
    )

    mockStorageInterface(storageInterfaceResponse)

    listerFn(allFiles(9).some).value.value should be(ListResponse[String](
      pathLocation,
      allFiles.slice(10, 20).map(_.file),
      allFiles(19),
    ))
    listerFn(allFiles(19).some).value.value should be(ListResponse[String](
      pathLocation,
      allFiles.slice(20, 30).map(_.file),
      allFiles(29),
    ))
  }
  private def mockStorageInterface(storageInterfaceResponse: ListResponse[FileMetadata]) =
    when(
      storageInterface.listRecursive(
        eqTo(pathLocation),
        any[(RemoteS3PathLocation, Seq[S3Object]) => Option[ListResponse[FileMetadata]]],
      ),
    ).thenReturn(storageInterfaceResponse.some.asRight)
}