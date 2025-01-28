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
package io.lenses.streamreactor.connect.cloud.common.formats.reader

import io.lenses.streamreactor.connect.cloud.common.formats.writer.JsonFormatWriter.getCompressionCodecIfSupported
import io.lenses.streamreactor.connect.cloud.common.model.CompressionCodec
import io.lenses.streamreactor.connect.cloud.common.model.CompressionCodecName.GZIP
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream

import java.io.InputStream

class JsonStreamReader(input: InputStream)(implicit compressionCodec: CompressionCodec)
    extends CloudDataIterator[String] {

  private val jsonCompressionCodec = getCompressionCodecIfSupported(compressionCodec)
  private val textStreamReader = new TextStreamReader(
    jsonCompressionCodec.compressionCodec match {
      case GZIP => new GzipCompressorInputStream(input)
      case _    => input
    },
  )

  override def close(): Unit = textStreamReader.close()

  override def next(): String = textStreamReader.next()

  override def hasNext: Boolean = textStreamReader.hasNext
}
