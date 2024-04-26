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
package io.lenses.streamreactor.connect.elastic.common.writers

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import cats.implicits._
import io.lenses.kcql.Kcql
import io.lenses.kcql.WriteModeEnum
import io.lenses.streamreactor.common.converters.FieldConverter
import io.lenses.streamreactor.common.schemas.ConverterUtil
import io.lenses.sql.Field
import com.typesafe.scalalogging.StrictLogging
import io.lenses.streamreactor.connect.elastic.common.client.ElasticClientWrapper
import io.lenses.streamreactor.connect.elastic.common.client.InsertRequest
import io.lenses.streamreactor.connect.elastic.common.client.Request
import io.lenses.streamreactor.connect.elastic.common.client.UpsertRequest
import io.lenses.streamreactor.connect.elastic.common.config.ElasticCommonSettings
import io.lenses.streamreactor.connect.elastic.common.indexname.CreateIndex
import io.lenses.streamreactor.connect.elastic.common.transform.Transform
import io.lenses.streamreactor.connect.elastic.common.transform.TransformAndExtractPK
import org.apache.kafka.connect.sink.SinkRecord

import java.util
import scala.annotation.nowarn
import scala.jdk.CollectionConverters.ListHasAsScala

@nowarn
class ElasticJsonWriter(client: ElasticClientWrapper, settings: ElasticCommonSettings)
    extends ElasticWriter
    with ConverterUtil
    with AutoCloseable
    with StrictLogging {

  logger.info("Initialising Elastic Json writer")

  //create the index automatically if it was set to do so
  settings.kcqls.filter(_.isAutoCreate).toList.map(CreateIndex.createIndex(_, client)).traverse(_.attempt).onError(t =>
    throw t,
  ).unsafeRunSync()

  private val topicKcqlMap = settings.kcqls.groupBy(_.getSource)

  private val kcqlMap = new util.IdentityHashMap[Kcql, KcqlValues]()
  settings.kcqls.foreach { kcql =>
    kcqlMap.put(
      kcql,
      KcqlValues(
        kcql.getFields.asScala.map(FieldConverter.apply).toSeq,
        kcql.getIgnoredFields.asScala.map(FieldConverter.apply).toSeq,
        kcql.getPrimaryKeys.asScala.map { pk =>
          val path = Option(pk.getParentFields).map(_.asScala.toVector).getOrElse(Vector.empty)
          path :+ pk.getName
        }.toSeq,
      ),
    )

  }

  /**
    * Close elastic4s client
    */
  override def close(): Unit = client.close()

  /**
    * Write SinkRecords to Elastic Search if list is not empty
    *
    * @param records A list of SinkRecords
    */
  override def write(records: Vector[SinkRecord]): IO[Unit] =
    if (records.isEmpty) {
      logger.debug("No records received.")
      IO.unit
    } else {
      logger.debug(s"Received ${records.size} records.")
      val grouped = records.groupBy(_.topic())
      insert(grouped)
    }

  /**
    * Create a bulk index statement and execute against elastic4s client
    *
    * @param records A list of SinkRecords
    */
  private def insert(records: Map[String, Vector[SinkRecord]]): IO[Unit] =
    records.flatMap {
      case (topic, sinkRecords) =>
        val kcqls = topicKcqlMap.getOrElse(
          topic,
          throw new IllegalArgumentException(
            s"$topic hasn't been configured in KCQL. Configured topics is ${topicKcqlMap.keys.mkString(",")}",
          ),
        )

        //we might have multiple inserts from the same Kafka Message
        kcqls.flatMap { kcql =>
          val i         = CreateIndex.getIndexName(kcql)
          val kcqlValue = kcqlMap.get(kcql)
          sinkRecords.grouped(settings.batchSize)
            .map { batch =>
              val indexes: Seq[Request] = batch.map { r =>
                val (json, pks) = if (kcqlValue.primaryKeysPath.isEmpty) {
                  (Transform(
                     kcqlValue.fields,
                     r.valueSchema(),
                     r.value(),
                     kcql.hasRetainStructure,
                   ),
                   Seq.empty,
                  )
                } else {
                  TransformAndExtractPK(
                    kcqlValue.fields,
                    kcqlValue.primaryKeysPath,
                    r.valueSchema(),
                    r.value(),
                    kcql.hasRetainStructure,
                  )
                }
                val idFromPk = pks.mkString(settings.pkJoinerSeparator)

                kcql.getWriteMode match {
                  case WriteModeEnum.INSERT =>
                    val id = if (idFromPk.isEmpty) autoGenId(r) else idFromPk
                    InsertRequest(i, id, json, kcql.getPipeline)

                  case WriteModeEnum.UPSERT =>
                    UpsertRequest(i, idFromPk, json)
                }
              }

              client.execute(indexes)
            }
        }
    }.toList.traverse(identity).void

  /**
    * Create id from record infos
    *
    * @param record One SinkRecord
    */
  private def autoGenId(record: SinkRecord): String = {
    val pks: Seq[Any] = Seq(record.topic(), record.kafkaPartition(), record.kafkaOffset())
    pks.mkString(settings.pkJoinerSeparator)
  }

  private case class KcqlValues(fields: Seq[Field], ignoredFields: Seq[Field], primaryKeysPath: Seq[Vector[String]])

}
