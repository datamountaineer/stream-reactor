/*
 * Copyright 2017-2024 Lenses.io Ltd
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
package io.lenses.streamreactor.connect.gcp.pubsub.source.configdef;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.apache.kafka.common.config.ConfigDef;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

import lombok.val;

@ExtendWith(MockitoExtension.class)
class PubSubConfigDefTest {

  @Test
  void configDefShouldContainGcpSettings() {
    val projectIdConfigSetting = PubSubConfigDef.getConfigDef().configKeys().get("connect.pubsub.gcp.project.id");
    assertEquals("", projectIdConfigSetting.defaultValue);
    assertEquals(ConfigDef.Type.STRING, projectIdConfigSetting.type);
    assertEquals(ConfigDef.Importance.HIGH, projectIdConfigSetting.importance);
    assertEquals("GCP Project ID", projectIdConfigSetting.documentation);
  }

  @Test
  void configDefShouldContainKcqlSettings() {
    val kcqlConfigSetting = PubSubConfigDef.getConfigDef().configKeys().get("connect.pubsub.kcql");
    assertEquals(ConfigDef.Type.STRING, kcqlConfigSetting.type);
    assertEquals(ConfigDef.Importance.HIGH, kcqlConfigSetting.importance);
    assertEquals("Contains the Kafka Connect Query Language describing the flow from the source to the target system.",
        kcqlConfigSetting.documentation);
  }

}
