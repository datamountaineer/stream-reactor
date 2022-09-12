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

import java.util.concurrent.TimeUnit
import com.datamountaineer.kcql.Field
import com.datamountaineer.kcql.Kcql
import com.datamountaineer.kcql.Tag
import com.datamountaineer.streamreactor.connect.influx2.NanoClock
import com.datamountaineer.streamreactor.connect.influx2.config.InfluxSettings
import com.datamountaineer.streamreactor.connect.influx2.converters.InfluxPoint
import com.datamountaineer.streamreactor.connect.influx2.converters.SinkRecordParser
import com.datamountaineer.streamreactor.connect.influx2.helpers.Util
import com.datamountaineer.streamreactor.connect.influx2.writers.KcqlDetails._
import com.typesafe.scalalogging.StrictLogging
import org.apache.kafka.common.config.ConfigException
import org.apache.kafka.connect.sink.SinkRecord
import com.datamountaineer.streamreactor.connect.influx2.javadto.BatchPoints
import com.influxdb.client.write.Point

import scala.jdk.CollectionConverters.ListHasAsScala
import scala.util.Failure
import scala.util.Success
import scala.util.Try

/**
  * Builds an InfluxDb point for the incoming SinkRecord.
  * It handles SinkRecord payloads as :
  *   - Kafka Connect Struct
  *   - Java Map
  *   - Json String
  *
  * @param settings - An instance of [[InfluxSettings]]
  */
class InfluxBatchPointsBuilder(settings: InfluxSettings, nanoClock: NanoClock) extends StrictLogging {
  private val kcqlMap: Map[String, Seq[KcqlDetails]] = settings.kcqls.groupBy(_.getSource).map {
    case (topic, kcqls) =>
      topic -> kcqls.map { kcql =>
        val timestampField = (Option(kcql.getTimestamp) match {
          case Some(Kcql.TIMESTAMP) => None
          case other                => other
        }).map(_.split('.')).map(_.toSeq)

        val allFields = readKeyFieldsFromKcql(kcql) ++ readFieldsFromKcql(kcql)

        val ignores = readIgnoredFieldsFromKcql(kcql)

        val tags = Option(kcql.getTags.asScala).toList.flatten.map { tag =>
          tag.getType match {
            case Tag.TagType.ALIAS    => DynamicTag(FieldName(tag.getValue), Path(tag.getKey.split('.').toList))
            case Tag.TagType.CONSTANT => ConstantTag(FieldName(tag.getKey), tag.getValue)
            case Tag.TagType.DEFAULT  => DynamicTag(FieldName(tag.getKey), Path(tag.getKey.split('.').toList))
          }
        }

        val dynamicTarget = Option(kcql.getDynamicTarget).map(_.split('.').toVector)
        val target        = kcql.getTarget

        KcqlDetails(allFields,
                    ignores,
                    tags,
                    dynamicTarget.map(Path),
                    target,
                    timestampField.map(Path),
                    kcql.getTimestampUnit,
        )
      }
  }

  private def readIgnoredFieldsFromKcql(kcql: Kcql) =
    kcql.getIgnoredFields.asScala.toList.map(field =>
      (FieldName(field.getName), mapParentFieldsToField(field), Option(field.getAlias).map(Alias)),
    ).toSet

  private def readFieldsFromKcql(kcql: Kcql) =
    kcql.getFields.asScala.toList.map { field =>
      (
        FieldName(Option(field.getAlias).getOrElse(field.getName)),
        mapParentFieldsToField(field),
        Option(field.getAlias).filter(_ != field.getName).map(Alias),
      )
    }

  private def mapParentFieldsToField(field: Field) =
    Path(Option(field.getParentFields).map(_.asScala.toList).getOrElse(List.empty[String]) ::: field.getName :: Nil)

  private def readKeyFieldsFromKcql(kcql: Kcql) =
    kcql.getKeyFields.asScala.toList.map {
      field =>
        (
          FieldName(Option(field.getAlias).getOrElse(field.getName)),
          Path(Option(field.getParentFields).map(_.asScala.toList).getOrElse(List.empty[String]) ++ Seq("_key",
                                                                                                        field.getName,
          )),
          Option(field.getAlias).filter(_ != field.getName).map(Alias),
        )
    }

  def build(records: Iterable[SinkRecord]): Try[BatchPoints] = {
    val batchPoints: BatchPoints = BatchPoints.Builder
      .bucket(settings.bucket)
      .consistency(settings.consistencyLevel)
      .build()

    records
      .map { record =>
        SinkRecordParser
          .build(record)
          .flatMap { parsedRecord =>
            kcqlMap
              .get(record.topic())
              .map(kcqls => kcqls.map(kcql => InfluxPoint.build(nanoClock)(parsedRecord, kcql)))
              .map(Success(_))
              .getOrElse(Failure(new ConfigException(s"Topic '${record.topic()}' is missing KCQL mapping.")))
          }
          .flatMap(_.foldLeft(Try(Seq.empty[Point]))(Util.shortCircuitOnFailure))
      }
      .foldLeft(Try(Seq.empty[Seq[Point]]))(Util.shortCircuitOnFailure)
      .map(_.flatten)
      .map { points =>
        batchPoints.setPoints(points)
      }
  }
}

/**
  * Builds a cache of fields path (fields to select, fields to ignore, tags and timestamp)
  */
case class KcqlDetails(
  fields:         Seq[(FieldName, Path, Option[Alias])],
  ignoredFields:  Set[(FieldName, Path, Option[Alias])],
  tags:           Seq[ParsedTag],
  dynamicTarget:  Option[Path],
  target:         String,
  timestampField: Option[Path],
  timestampUnit:  TimeUnit
) {
  val IgnoredAndAliasFields: Set[Path] =
    fields
      .filter {
        case (_, _, Some(_)) => true
        case (_, _, None)    => false
      }
      .map {
        case (_, path, _) => path
      }.toSet ++ ignoredFields.map { case (_, path, _) => path }

  val NonIgnoredFields: Seq[(FieldName, Path, Option[Alias])] = fields
    .filterNot {
      case (_, path, _) => ignoredFields.exists(_._2 == path)
    }
}

object KcqlDetails {

  case class Path(value: Seq[String]) {
    def equals(path: Path): Boolean = Util.caseInsensitiveComparison(value, path.value)
  }

  case class FieldName(value: String) extends AnyVal

  case class Alias(value: String) extends AnyVal

  sealed trait ParsedTag {
    def name: FieldName
  }

  case class ConstantTag(name: FieldName, value: String) extends ParsedTag

  case class DynamicTag(name: FieldName, path: Path) extends ParsedTag

}
