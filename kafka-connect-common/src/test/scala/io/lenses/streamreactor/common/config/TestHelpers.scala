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
package io.lenses.streamreactor.common.config

import io.lenses.streamreactor.common.TestUtilsBase
import org.apache.kafka.common.config.ConfigException
import org.scalatest.EitherValues
import org.scalatest.matchers.should.Matchers

/**
  * Created by andrew@datamountaineer.com on 23/08/2017.
  * kafka-connect-common
  */
class TestHelpers extends TestUtilsBase with EitherValues with Matchers {

  val kcqlConstant: String = "myconnector.kcql"

  "should throw exception if topics not specified in connector props" in {
    val props = Map("topics" -> "t1",
                    s"$kcqlConstant" -> "insert into table select  * from t1;insert into table2 select * from t2",
    )

    Helpers.checkInputTopics(kcqlConstant, props).left.value should be(a[ConfigException])

  }

  "should throw exception if topics not specified in kcql" in {
    val props = Map("topics" -> "t1,t2", s"$kcqlConstant" -> "insert into table select  * from t1")

    Helpers.checkInputTopics(kcqlConstant, props).left.value should be(a[ConfigException])

  }

  "should not throw exception if all good" in {
    val props = Map("topics" -> "t1,t2",
                    s"$kcqlConstant" -> "insert into table select  * from t1;insert into table2 select * from t2",
    )

    Helpers.checkInputTopics(kcqlConstant, props).value should be(())
  }

  "should add topics involved in kcql error to message" in {
    val props = Map("topics" -> "topic1",
                    s"$kcqlConstant" -> "insert into table select time,c1,c2 from topic1 WITH TIMESTAMP time",
    )

    val e = Helpers.checkInputTopics(kcqlConstant, props).left.value
    e should be(a[ConfigException])
    e.getMessage.contains("topic1WITHTIMESTAMPtime") shouldBe true
  }
}
