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
package io.lenses.kcql;

import cyclops.control.Option;
import io.lenses.kcql.targettype.KeyTargetType;
import lombok.val;
import org.junit.jupiter.api.Test;

import static io.lenses.streamreactor.test.utils.EitherValues.assertRight;

class KcqlTargetTypeTest {

  @Test
  void targetTypeWithEscapingShouldBeSupported() {

    String syntax = "INSERT INTO `_key.'field.1'.'field.2'` SELECT * FROM TOPIC_A";
    val targetType = Kcql.parse(syntax).getTargetType();

    assertRight(targetType).isEqualTo(new KeyTargetType(Option.of("'field.1'.'field.2'")));

  }

}