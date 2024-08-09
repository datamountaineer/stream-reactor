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
package io.lenses.streamreactor.connect.cloud.common.config

import io.lenses.streamreactor.common.config.base.traits.BaseSettings
import io.lenses.streamreactor.common.config.base.traits.WithConnectorPrefix
import io.lenses.streamreactor.connect.cloud.common.sink.config.IndexOptions
import org.apache.kafka.common.config.ConfigDef
import org.apache.kafka.common.config.ConfigDef.Importance
import org.apache.kafka.common.config.ConfigDef.Type

trait IndexConfigKeys extends WithConnectorPrefix {

  val SEEK_MAX_INDEX_FILES = s"$connectorPrefix.seek.max.files"
  private val SEEK_MAX_INDEX_FILES_DOC =
    s"Maximum index files to allow per topic/partition.  Advisable to not raise this: if a large number of files build up this means there is a problem with file deletion."
  private val SEEK_MAX_INDEX_FILES_DEFAULT = 5

  val INDEXES_DIRECTORY_NAME = s"$connectorPrefix.indexes.name"
  private val INDEXES_DIRECTORY_NAME_DOC =
    s"Name of the indexes directory"
  private val INDEXES_DIRECTORY_NAME_DEFAULT = ".indexes"

  def addIndexSettingsToConfigDef(configDef: ConfigDef): ConfigDef =
    configDef
      .define(
        SEEK_MAX_INDEX_FILES,
        Type.INT,
        SEEK_MAX_INDEX_FILES_DEFAULT,
        Importance.LOW,
        SEEK_MAX_INDEX_FILES_DOC,
        "Sink Seek",
        1,
        ConfigDef.Width.LONG,
        SEEK_MAX_INDEX_FILES,
      )
      .define(
        INDEXES_DIRECTORY_NAME,
        Type.STRING,
        INDEXES_DIRECTORY_NAME_DEFAULT,
        Importance.LOW,
        INDEXES_DIRECTORY_NAME_DOC,
        "Sink Seek",
        2,
        ConfigDef.Width.LONG,
        INDEXES_DIRECTORY_NAME,
      )
}
trait IndexSettings extends BaseSettings with IndexConfigKeys {
  def getIndexSettings: IndexOptions =
    IndexOptions(
      getInt(SEEK_MAX_INDEX_FILES),
      getString(INDEXES_DIRECTORY_NAME),
    )
}
