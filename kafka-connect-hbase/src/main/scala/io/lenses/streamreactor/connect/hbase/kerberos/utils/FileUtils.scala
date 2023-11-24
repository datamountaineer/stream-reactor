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
package io.lenses.streamreactor.connect.hbase.kerberos.utils

import java.io.File

import org.apache.kafka.common.config.ConfigException

object FileUtils {
  def throwIfNotExists(file: String, key: String): Unit = {
    val f = new File(file)
    if (!f.exists() || f.isDirectory) {
      throw new ConfigException(s"Cannot find the file [$file] provided for [$key] setting.")
    }
  }
}