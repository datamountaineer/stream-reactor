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
package io.lenses.streamreactor.connect.io.text

import java.io.BufferedReader
import java.io.InputStream
import java.io.InputStreamReader

/**
  * Reads records from an input stream. A record starts when a line matching start is found and ends when a line matching
  * end is found. The start and end lines are included in the record.
  * If the file ends and there is no end, the record is ignored
  *
  * @param input
  * @param start
  * @param end
  */
class LineStartLineEndReader(input: InputStream, start: String, end: String) extends LineReader{
  private val br      = new BufferedReader(new InputStreamReader(input))

  //Returns the next record or None if there are no more
  def next(): Option[String] = {
    if (readUntilStart()) {
      readUntilEndOrNone()
    } else None
  }

  def close(): Unit =
    input.close()

  private def readUntilStart(): Boolean = {
    var line = br.readLine()
    while (line != null && !line.equalsIgnoreCase(start)) {
      line = br.readLine()
    }
    Option(line).isDefined
  }

  private def readUntilEndOrNone(): Option[String] = {
    val builder = new StringBuilder()
    builder.append(start)
    var line = br.readLine()
    while (line != null && !line.equalsIgnoreCase(end)) {
      builder.append(System.lineSeparator())
      builder.append(line)
      line = br.readLine()
    }
    Option(line).map { _ =>
      builder.append(System.lineSeparator())
      builder.append(end)
      builder.toString()
    }
  }
}
