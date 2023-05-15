/*
 * Copyright 2020 Lenses.io
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

import io.lenses.streamreactor.connect.aws.s3.model.SchemaAndValueSourceData
import io.lenses.streamreactor.connect.aws.s3.model.location.RemoteS3PathLocation

import java.io.InputStream
import scala.io.Source
import scala.util.Try

import scala.jdk.CollectionConverters.MapHasAsJava
import org.apache.kafka.connect.json.JsonConverter

class JsonFormatStreamReader(inputStreamFn: () => InputStream, bucketAndPath: RemoteS3PathLocation)
  extends S3FormatStreamReader[SchemaAndValueSourceData] {

  private val inputStream: InputStream = inputStreamFn()
  private val source = Source.fromInputStream(inputStream, "UTF-8")
  protected val sourceLines = source.getLines()
  protected var lineNumber: Long = -1

  private val jsonConverter = new JsonConverter

  jsonConverter.configure(
    Map("schemas.enable" -> false).asJava,
    false,
  )

  override def close(): Unit = {
    val _ = Try(source.close())
  }

  override def hasNext: Boolean = sourceLines.hasNext


  override def next(): SchemaAndValueSourceData = {
    lineNumber += 1
    if (!sourceLines.hasNext) {
      throw FormatWriterException(
        "Invalid state reached: the file content has been consumed, no further calls to next() are possible.",
      )
    }
    val genericRecordData = sourceLines.next();
    val genericRecord = if (genericRecordData == null) null else genericRecordData.getBytes();
    val schemaAndValue = jsonConverter.toConnectData("", genericRecord)
    SchemaAndValueSourceData(schemaAndValue, lineNumber)
  }

  override def getBucketAndPath: RemoteS3PathLocation = bucketAndPath

  override def getLineNumber: Long = lineNumber
}
