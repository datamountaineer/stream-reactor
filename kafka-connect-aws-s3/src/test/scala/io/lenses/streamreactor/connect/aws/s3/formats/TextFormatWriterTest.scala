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

import io.lenses.streamreactor.connect.aws.s3.formats.writer.MessageDetail
import io.lenses.streamreactor.connect.aws.s3.formats.writer.StringSinkData
import io.lenses.streamreactor.connect.aws.s3.formats.writer.StructSinkData
import io.lenses.streamreactor.connect.aws.s3.formats.writer.TextFormatWriter
import io.lenses.streamreactor.connect.aws.s3.model.Offset
import io.lenses.streamreactor.connect.aws.s3.stream.S3ByteArrayOutputStream
import io.lenses.streamreactor.connect.aws.s3.utils.TestSampleSchemaAndData._
import org.scalatest.EitherValues
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.time.Instant

class TextFormatWriterTest extends AnyFlatSpec with Matchers with EitherValues {

  "convert" should "write byte output stream with text format for a single record" in {

    val outputStream     = new S3ByteArrayOutputStream()
    val jsonFormatWriter = new TextFormatWriter(outputStream)
    jsonFormatWriter.write(MessageDetail(None,
                                         StringSinkData("Sausages"),
                                         Map.empty,
                                         Some(Instant.now()),
                                         topic,
                                         0,
                                         Offset(0),
    ))

    outputStream.toString should be("Sausages\n")
    outputStream.getPointer should be(9L)

  }

  "convert" should "write byte output stream with json for multiple records" in {

    val outputStream     = new S3ByteArrayOutputStream()
    val jsonFormatWriter = new TextFormatWriter(outputStream)
    jsonFormatWriter.write(MessageDetail(None,
                                         StringSinkData("Sausages"),
                                         Map.empty,
                                         Some(Instant.now()),
                                         topic,
                                         0,
                                         Offset(0),
    ))
    jsonFormatWriter.write(MessageDetail(None,
                                         StringSinkData("Mash"),
                                         Map.empty,
                                         Some(Instant.now()),
                                         topic,
                                         0,
                                         Offset(1),
    ))
    jsonFormatWriter.write(MessageDetail(None,
                                         StringSinkData("Peas"),
                                         Map.empty,
                                         Some(Instant.now()),
                                         topic,
                                         0,
                                         Offset(2),
    ))

    outputStream.toString should be("Sausages\nMash\nPeas\n")

  }

  "convert" should "throw error when avro value is supplied" in {

    val outputStream     = new S3ByteArrayOutputStream()
    val textFormatWriter = new TextFormatWriter(outputStream)
    val caught =
      textFormatWriter.write(MessageDetail(None,
                                           StructSinkData(users.head),
                                           Map.empty,
                                           Some(Instant.now()),
                                           topic,
                                           0,
                                           Offset(0),
      ))
    caught.left.value shouldBe a[FormatWriterException]
  }
}
