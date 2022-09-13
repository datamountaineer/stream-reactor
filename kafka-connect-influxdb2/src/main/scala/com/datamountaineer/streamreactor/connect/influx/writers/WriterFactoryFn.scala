/*
 * Copyright 2017 Datamountaineer.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.datamountaineer.streamreactor.connect.influx2.writers

import com.datamountaineer.streamreactor.connect.influx2.config.InfluxSettings
import com.typesafe.scalalogging.StrictLogging

/**
  * Provides the logic for instantiating the appropriate InfluxDb writer
  */
object WriterFactoryFn extends StrictLogging {

  /**
    * Creates the InfluxDb writer which corresponds to the given settings
    *
    * @param settings InfluxSetting for the writer
    * @return
    */
  def apply(settings: InfluxSettings): InfluxDbWriter = new InfluxDbWriter(settings)

}
