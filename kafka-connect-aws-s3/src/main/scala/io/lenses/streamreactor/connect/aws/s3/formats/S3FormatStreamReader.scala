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
package io.lenses.streamreactor.connect.aws.s3.formats

import io.lenses.streamreactor.connect.aws.s3.config.FormatOptions.WithHeaders
import io.lenses.streamreactor.connect.aws.s3.config.Format
import io.lenses.streamreactor.connect.aws.s3.config.FormatSelection
import io.lenses.streamreactor.connect.aws.s3.model.SourceData
import io.lenses.streamreactor.connect.aws.s3.model.location.RemoteS3PathLocation

import java.io.InputStream

object S3FormatStreamReader {

  def apply(
    inputStreamFn: () => InputStream,
    fileSizeFn:    () => Long,
    format:        FormatSelection,
    bucketAndPath: RemoteS3PathLocation,
  ): S3FormatStreamReader[_ <: SourceData] =
    format.format match {
      case Format.Avro    => new AvroFormatStreamReader(inputStreamFn, bucketAndPath)
      case Format.Json    => new JsonFormatStreamReader(inputStreamFn, bucketAndPath)
      case Format.Text    => new TextFormatStreamReader(inputStreamFn, bucketAndPath)
      case Format.Parquet => new ParquetFormatStreamReader(inputStreamFn, fileSizeFn, bucketAndPath)
      case Format.Csv =>
        new CsvFormatStreamReader(inputStreamFn, bucketAndPath, hasHeaders = format.formatOptions.contains(WithHeaders))
      case Format.Bytes =>
        val bytesWriteMode = S3FormatWriter.convertToBytesWriteMode(format.formatOptions)
        if (bytesWriteMode.entryName.toLowerCase.contains("size")) {
          new BytesFormatWithSizesStreamReader(inputStreamFn, fileSizeFn, bucketAndPath, bytesWriteMode)
        } else {
          new BytesFormatStreamFileReader(inputStreamFn, fileSizeFn, bucketAndPath, bytesWriteMode)
        }
    }
}

trait S3FormatStreamReader[R <: SourceData] extends AutoCloseable with Iterator[R] {

  def getBucketAndPath: RemoteS3PathLocation

  def getLineNumber: Long

}
