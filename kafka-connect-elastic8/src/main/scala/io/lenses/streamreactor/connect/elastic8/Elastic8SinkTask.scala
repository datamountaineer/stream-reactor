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
package io.lenses.streamreactor.connect.elastic8

import io.lenses.streamreactor.connect.elastic.common.ElasticSinkTask
import io.lenses.streamreactor.connect.elastic8.config.Elastic8ConfigDef
import io.lenses.streamreactor.connect.elastic8.config.Elastic8Settings
import io.lenses.streamreactor.connect.elastic8.config.Elastic8SettingsReader
import io.lenses.streamreactor.connect.elastic8.writers.Elastic8ClientCreator

class Elastic8SinkTask
    extends ElasticSinkTask[Elastic8Settings, Elastic8ConfigDef](
      Elastic8SettingsReader,
      Elastic8ClientCreator,
      new Elastic8ConfigDef(),
      "/elastic-ascii.txt",
    ) {}
