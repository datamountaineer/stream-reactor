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

import com.typesafe.scalalogging.LazyLogging
import io.lenses.streamreactor.connect.aws.s3.model.location.RemoteS3PathLocation
import io.lenses.streamreactor.connect.aws.s3.model.location.RemoteS3RootLocation
import io.lenses.streamreactor.connect.aws.s3.storage.StorageInterface

/**
  * The [[S3SourceLister]] is responsible for querying the [[StorageInterface]] to
  * retrieve a list of S3 topics and partitions for reading
  */
class S3SourceLister extends LazyLogging {

  def listBatch(
    bucketAndPrefix: RemoteS3RootLocation,
    lastFile:        Option[RemoteS3PathLocation],
    numResults:      Int,
  )(
    implicit
    storageInterface: StorageInterface,
  ): Either[Throwable, List[RemoteS3PathLocation]] =
    storageInterface.list(bucketAndPrefix, lastFile, numResults).map(_.map(bucketAndPrefix.withPath))
}
