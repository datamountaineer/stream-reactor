/*
 * Copyright 2017-2025 Lenses.io Ltd
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
package io.lenses.streamreactor.connect.aws.s3.source.state

import cats.effect.testing.scalatest.AsyncIOSpec
import cats.implicits.catsSyntaxEitherId
import io.lenses.streamreactor.connect.aws.s3.model.location.S3LocationValidator
import io.lenses.streamreactor.connect.aws.s3.storage.S3FileMetadata
import io.lenses.streamreactor.connect.cloud.common.config.AvroFormatSelection
import io.lenses.streamreactor.connect.cloud.common.config.ConnectorTaskId
import io.lenses.streamreactor.connect.cloud.common.config.traits.CloudSourceConfig
import io.lenses.streamreactor.connect.cloud.common.model.location.CloudLocation
import io.lenses.streamreactor.connect.cloud.common.model.location.CloudLocationValidator
import io.lenses.streamreactor.connect.cloud.common.source.config.CloudSourceBucketOptions
import io.lenses.streamreactor.connect.cloud.common.source.config.EmptySourceBackoffSettings
import io.lenses.streamreactor.connect.cloud.common.source.config.OrderingType
import io.lenses.streamreactor.connect.cloud.common.source.state.ReaderManagerBuilder
import io.lenses.streamreactor.connect.cloud.common.storage.FileListError
import io.lenses.streamreactor.connect.cloud.common.storage.FileLoadError
import io.lenses.streamreactor.connect.cloud.common.storage.StorageInterface
import org.mockito.Mockito.when
import org.mockito.MockitoSugar.mock
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should.Matchers

class ReaderManagerBuilderTest extends AsyncFlatSpec with AsyncIOSpec with Matchers {
  implicit val cloudLocationValidator: CloudLocationValidator = S3LocationValidator
  "ReaderManagerBuilder" should "create a reader manager" in {
    val si     = mock[StorageInterface[S3FileMetadata]]
    val path   = "prefix1/subprefixA/subprefixB/"
    val config = mock[CloudSourceConfig[S3FileMetadata]]

    val root = CloudLocation(
      "bucket",
      None,
      Some(path),
      None,
      None,
    )
    when(si.pathExists(root.bucket, root.path.get)).thenReturn(true.asRight[FileLoadError])

    var rootValue: Option[CloudLocation] = None
    val contextF: CloudLocation => Option[CloudLocation] = { in =>
      rootValue = Some(in)
      rootValue
    }
    val sbo = CloudSourceBucketOptions[S3FileMetadata](root,
                                                       "topic",
                                                       AvroFormatSelection,
                                                       100,
                                                       100,
                                                       None,
                                                       OrderingType.LastModified,
                                                       false,
                                                       Option.empty,
    )
    val taskId       = ConnectorTaskId("test", 3, 1)
    val pathLocation = root.withPath(path)
    ReaderManagerBuilder(root,
                         pathLocation,
                         config.compressionCodec,
                         si,
                         taskId,
                         contextF,
                         _ => Some(sbo),
                         EmptySourceBackoffSettings(1, 1, 2.0),
                         true,
    )
      .asserting(_ => rootValue shouldBe Some(pathLocation))
  }

  //handle the case where the location does not exist
  it should "not fail if the location does not exist" in {
    val si     = mock[StorageInterface[S3FileMetadata]]
    val path   = "prefix1/subprefixA/subprefixB/"
    val config = mock[CloudSourceConfig[S3FileMetadata]]

    val root = CloudLocation(
      "bucket",
      None,
      Some(path),
      None,
      None,
    )
    when(si.listFileMetaRecursive(root.bucket, root.prefix)).thenReturn(None.asRight[FileListError])
    when(si.pathExists(root.bucket, root.path.get)).thenReturn(false.asRight[FileLoadError])

    var rootValue: Option[CloudLocation] = None
    val contextF: CloudLocation => Option[CloudLocation] = { in =>
      rootValue = Some(in)
      rootValue
    }
    val sbo = CloudSourceBucketOptions[S3FileMetadata](root,
                                                       "topic",
                                                       AvroFormatSelection,
                                                       100,
                                                       100,
                                                       None,
                                                       OrderingType.LastModified,
                                                       false,
                                                       Option.empty,
    )
    val taskId       = ConnectorTaskId("test", 3, 1)
    val pathLocation = root.withPath(path)
    ReaderManagerBuilder.apply(root,
                               pathLocation,
                               config.compressionCodec,
                               si,
                               taskId,
                               contextF,
                               _ => Some(sbo),
                               EmptySourceBackoffSettings(1, 1, 2.0),
                               true,
    )
      .flatMap(_.poll())
      .asserting { result =>
        result shouldBe empty
      }
  }
}
