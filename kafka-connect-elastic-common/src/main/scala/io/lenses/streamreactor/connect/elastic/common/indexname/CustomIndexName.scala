/*
 * Copyright 2017-2024 Lenses.io Ltd
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
package io.lenses.streamreactor.connect.elastic.common.indexname

import scala.annotation.tailrec

class InvalidCustomIndexNameException(message: String) extends RuntimeException(message)

case class CustomIndexName(fragments: Vector[IndexNameFragment]) {
  override def toString: String = fragments.map(_.getFragment).mkString
}

object CustomIndexName {

  @tailrec
  private def parseIndexName(
    remainingChars:  Vector[Char],
    currentFragment: StringBuilder,
    results:         Vector[Option[IndexNameFragment]],
  ): Vector[IndexNameFragment] =
    remainingChars match {
      case head +: rest => head match {
          case DateTimeFragment.OpeningChar =>
            val (dateTimeFormat, afterDateTimeFormatIncludingClosingChar) = rest.span {
              _ != DateTimeFragment.ClosingChar
            }
            val afterDateTimeFormat = afterDateTimeFormatIncludingClosingChar.tail

            val maybeCurrentFragment = currentFragment.mkString.toOption
            val maybeDateTimeFormat  = dateTimeFormat.mkString.toOption

            val newResultsWithDateTimeFragment =
              results :+ maybeCurrentFragment.map(TextFragment.apply) :+ maybeDateTimeFormat.map(DateTimeFragment(_))

            parseIndexName(afterDateTimeFormat, new StringBuilder, newResultsWithDateTimeFragment)
          case DateTimeFragment.ClosingChar =>
            throw new InvalidCustomIndexNameException(
              s"Found closing '${DateTimeFragment.ClosingChar}' but no opening character",
            )
          case anyOtherChar => parseIndexName(rest, currentFragment.append(anyOtherChar), results)
        }
      case Vector() =>
        val maybeCurrentFragment = currentFragment.mkString.toOption
        (results :+ maybeCurrentFragment.map(TextFragment.apply)).flatten
      case other => throw new IllegalStateException(s"Invalid match for $other")
    }

  def parseIndexName(indexName: String): CustomIndexName =
    CustomIndexName(parseIndexName(indexName.toVector, new StringBuilder, Vector.empty))
}
