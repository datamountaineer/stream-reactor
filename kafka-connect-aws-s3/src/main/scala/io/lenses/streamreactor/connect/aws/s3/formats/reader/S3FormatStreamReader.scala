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
package io.lenses.streamreactor.connect.aws.s3.formats.reader

import io.lenses.streamreactor.connect.aws.s3.config._
import io.lenses.streamreactor.connect.aws.s3.model.location.S3Location

import java.io.InputStream

object S3FormatStreamReader {

  def apply(
    inputStream:          InputStream,
    fileSize:             Long,
    formatSelection:      FormatSelection,
    bucketAndPath:        S3Location,
    recreateInputStreamF: () => Either[Throwable, InputStream],
  ): S3FormatStreamReader[_ <: SourceData] =
    formatSelection.toStreamReader(inputStream, fileSize, bucketAndPath, recreateInputStreamF)
}

trait S3FormatStreamReader[R <: SourceData] extends AutoCloseable with Iterator[R] {

  def getBucketAndPath: S3Location

  def getLineNumber: Long

}
