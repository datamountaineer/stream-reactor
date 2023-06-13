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
package io.lenses.streamreactor.connect.aws.s3.config

import enumeratum.Enum
import enumeratum.EnumEntry

import scala.collection.immutable

sealed trait Format extends EnumEntry

object Format extends Enum[Format] {
  override val values: immutable.IndexedSeq[Format] = findValues
  case object Json    extends Format
  case object Avro    extends Format
  case object Parquet extends Format
  case object Text    extends Format
  case object Csv     extends Format
  case object Bytes   extends Format
}