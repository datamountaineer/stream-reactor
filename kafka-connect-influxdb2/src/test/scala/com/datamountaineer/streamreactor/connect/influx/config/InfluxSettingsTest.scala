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

package com.datamountaineer.streamreactor.connect.influx.config

import com.datamountaineer.kcql.Kcql
import com.datamountaineer.streamreactor.common.errors.ThrowErrorPolicy
import org.apache.kafka.common.config.ConfigException
import com.influxdb.InfluxDB.ConsistencyLevel
import org.mockito.MockitoSugar
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import scala.jdk.CollectionConverters.{ListHasAsScala, MapHasAsJava}



class InfluxSettingsTest extends AnyWordSpec with Matchers with MockitoSugar {

  val MEASURE_NAME = "someMeasurement"
  val TOPIC_NAME = "mykafkatopic"
  val QUERY_ALL = s"INSERT INTO $MEASURE_NAME SELECT * FROM $TOPIC_NAME"
  val QUERY_SELECT = s"INSERT INTO $MEASURE_NAME SELECT lastName as surname, firstName FROM $TOPIC_NAME"
  val QUERY_SELECT_AND_TIMESTAMP = s"INSERT INTO $MEASURE_NAME SELECT * FROM $TOPIC_NAME WITHTIMESTAMP ts"
  val QUERY_SELECT_AND_TIMESTAMP_SYSTEM = s"INSERT INTO $MEASURE_NAME SELECT * FROM $TOPIC_NAME WITHTIMESTAMP ${Kcql.TIMESTAMP}"

  "raise a configuration exception if the connection url is missing" in {
    intercept[ConfigException] {
      val props = Map(
        InfluxConfigConstants.INFLUX_DATABASE_CONFIG -> "mydb",
        InfluxConfigConstants.INFLUX_CONNECTION_USER_CONFIG -> "myuser",
        InfluxConfigConstants.INFLUX_CONNECTION_PASSWORD_CONFIG -> "apass",
        InfluxConfigConstants.KCQL_CONFIG -> QUERY_ALL
      ).asJava

      val config = InfluxConfig(props)
      InfluxSettings(config)
    }
  }

  "raise a configuration exception if the database is not set" in {
    intercept[ConfigException] {
      val props = Map(
        InfluxConfigConstants.INFLUX_URL_CONFIG -> "http://localhost:8081",
        InfluxConfigConstants.INFLUX_DATABASE_CONFIG -> "",
        InfluxConfigConstants.INFLUX_CONNECTION_USER_CONFIG -> "myuser",
        InfluxConfigConstants.INFLUX_CONNECTION_PASSWORD_CONFIG -> "apass",
        InfluxConfigConstants.KCQL_CONFIG -> QUERY_ALL
      ).asJava

      val config = InfluxConfig(props)
      InfluxSettings(config)
    }
  }

  "raise a configuration exception if the Consistency Level is wrong" in {
    intercept[ConfigException] {
      val url = "http://localhost:8081"
      val database = "mydatabase"
      val user = "myuser"

      val props = Map(
        InfluxConfigConstants.INFLUX_URL_CONFIG -> url,
        InfluxConfigConstants.INFLUX_DATABASE_CONFIG -> database,
        InfluxConfigConstants.INFLUX_CONNECTION_USER_CONFIG -> user,
        InfluxConfigConstants.KCQL_CONFIG -> QUERY_ALL,
        InfluxConfigConstants.CONSISTENCY_CONFIG -> "SOMELEVEL"
      ).asJava

      val config = InfluxConfig(props)
      InfluxSettings(config)
    }
  }

  "raise a configuration exception if the user is not set" in {
    intercept[ConfigException] {
      val url = "http://localhost:8081"
      val database = "mydatabase"

      val props = Map(
        InfluxConfigConstants.INFLUX_URL_CONFIG -> url,
        InfluxConfigConstants.INFLUX_DATABASE_CONFIG -> database,
        InfluxConfigConstants.INFLUX_CONNECTION_USER_CONFIG -> "",
        InfluxConfigConstants.KCQL_CONFIG -> QUERY_ALL,
        InfluxConfigConstants.INFLUX_CONNECTION_PASSWORD_CONFIG -> "apass"
      ).asJava

      val config = InfluxConfig(props)
      InfluxSettings(config)
    }
  }

  "create a settings with all fields" in {
    val url = "http://localhost:8081"
    val database = "mydatabase"
    val user = "myuser"

    val props = Map(
      InfluxConfigConstants.INFLUX_URL_CONFIG -> url,
      InfluxConfigConstants.INFLUX_DATABASE_CONFIG -> database,
      InfluxConfigConstants.INFLUX_CONNECTION_USER_CONFIG -> user,
      InfluxConfigConstants.KCQL_CONFIG -> QUERY_ALL,
      InfluxConfigConstants.CONSISTENCY_CONFIG -> ConsistencyLevel.QUORUM.toString
    ).asJava

    val config = InfluxConfig(props)

    val settings = InfluxSettings(config)
    settings.connectionUrl shouldBe url
    settings.database shouldBe database
    settings.user shouldBe user
    settings.password shouldBe null
    settings.errorPolicy shouldBe ThrowErrorPolicy()
    settings.kcqls.size shouldBe 1
    settings.kcqls.head.getTimestamp shouldBe null
  }

  "create a settings with selected fields" in {
    val url = "http://localhost:8081"
    val database = "mydatabase"
    val user = "myuser"
    val pass = "mememe"

    val props = Map(
      InfluxConfigConstants.INFLUX_URL_CONFIG -> url,
      InfluxConfigConstants.INFLUX_DATABASE_CONFIG -> database,
      InfluxConfigConstants.INFLUX_CONNECTION_USER_CONFIG -> user,
      InfluxConfigConstants.INFLUX_CONNECTION_PASSWORD_CONFIG -> pass,
      InfluxConfigConstants.KCQL_CONFIG -> QUERY_SELECT,
      InfluxConfigConstants.CONSISTENCY_CONFIG -> ConsistencyLevel.ANY.toString
    ).asJava

    val config = InfluxConfig(props)

    val settings = InfluxSettings(config)
    settings.connectionUrl shouldBe url
    settings.database shouldBe database
    settings.user shouldBe user
    settings.password shouldBe pass
    settings.errorPolicy shouldBe ThrowErrorPolicy()
    settings.kcqls.size shouldBe 1
    settings.kcqls.head.getFields.asScala.exists(_.getName == "*") shouldBe false
    settings.kcqls.head.getTimestamp shouldBe null
  }

  "create a settings with selected fields with timestamp set to a field" in {
    val url = "http://localhost:8081"
    val database = "mydatabase"
    val user = "myuser"
    val pass = "mememe"

    val props = Map(
      InfluxConfigConstants.INFLUX_URL_CONFIG -> url,
      InfluxConfigConstants.INFLUX_DATABASE_CONFIG -> database,
      InfluxConfigConstants.INFLUX_CONNECTION_USER_CONFIG -> user,
      InfluxConfigConstants.INFLUX_CONNECTION_PASSWORD_CONFIG -> pass,
      InfluxConfigConstants.KCQL_CONFIG -> QUERY_SELECT_AND_TIMESTAMP,
      InfluxConfigConstants.CONSISTENCY_CONFIG -> ConsistencyLevel.ONE.toString
    ).asJava

    val config = InfluxConfig(props)

    val settings = InfluxSettings(config)
    settings.connectionUrl shouldBe url
    settings.database shouldBe database
    settings.user shouldBe user
    settings.password shouldBe pass
    settings.errorPolicy shouldBe ThrowErrorPolicy()
    settings.kcqls.size shouldBe 1
    settings.kcqls.head.getFields.asScala.exists(_.getName == "*") shouldBe true
    settings.kcqls.head.getTimestamp shouldBe "ts"
    settings.consistencyLevel shouldBe ConsistencyLevel.ONE
  }

  "create a settings with selected fields with timestamp set to a sys_timestamp" in {
    val url = "http://localhost:8081"
    val database = "mydatabase"
    val user = "myuser"
    val pass = "mememe"

    val props = Map(
      InfluxConfigConstants.INFLUX_URL_CONFIG -> url,
      InfluxConfigConstants.INFLUX_DATABASE_CONFIG -> database,
      InfluxConfigConstants.INFLUX_CONNECTION_USER_CONFIG -> user,
      InfluxConfigConstants.INFLUX_CONNECTION_PASSWORD_CONFIG -> pass,
      InfluxConfigConstants.KCQL_CONFIG -> QUERY_SELECT_AND_TIMESTAMP_SYSTEM,
      InfluxConfigConstants.CONSISTENCY_CONFIG -> ConsistencyLevel.ONE.toString
    ).asJava

    val config = InfluxConfig(props)


    val settings = InfluxSettings(config)
    settings.connectionUrl shouldBe url
    settings.database shouldBe database
    settings.user shouldBe user
    settings.password shouldBe pass
    settings.errorPolicy shouldBe ThrowErrorPolicy()
    settings.kcqls.size shouldBe 1
    settings.kcqls.head.getFields.asScala.exists(_.getName == "*") shouldBe true
    settings.kcqls.head.getTimestamp shouldBe Kcql.TIMESTAMP
  }
}
