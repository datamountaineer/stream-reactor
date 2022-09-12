package com.datamountaineer.streamreactor.connect.influx2.converters

import com.datamountaineer.streamreactor.connect.influx2.helpers.Util
import com.datamountaineer.streamreactor.connect.influx2.writers.KcqlDetails.Path
import com.datamountaineer.streamreactor.connect.influx2.writers.ValuesExtractor
import com.fasterxml.jackson.databind.JsonNode
import com.landoop.json.sql.JacksonJson
import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.data.Struct
import org.apache.kafka.connect.sink.SinkRecord

import scala.util.Try

object SinkRecordParser {
  type Field = String

  trait ParsedSinkRecord {
    def valueFields(ignored: Set[Path]): Seq[(String, Any)]

    def field(path: Path): Option[Any]
  }

  trait ParsedKeyValueSinkRecord extends ParsedSinkRecord {
    def keyFields(ignored: Set[Path]): Seq[(String, Any)]
  }

  private case class JsonSinkRecord(json: JsonNode) extends ParsedSinkRecord {
    override def valueFields(ignored: Set[Path]): Seq[(String, Any)] =
      ValuesExtractor.extractAllFields(json, ignored.map(_.value.last))

    override def field(path: Path): Option[Any] = Option(ValuesExtractor.extract(json, path.value))
  }

  private case class StructSinkRecord(struct: Struct) extends ParsedSinkRecord {
    override def valueFields(ignored: Set[Path]): Seq[(String, Any)] =
      ValuesExtractor.extractAllFields(struct, ignored.map(_.value.last))

    override def field(path: Path): Option[Any] = Option(ValuesExtractor.extract(struct, path.value))
  }

  private case class MapSinkRecord(map: java.util.Map[String, Any]) extends ParsedSinkRecord {
    override def valueFields(ignored: Set[Path]): Seq[(String, Any)] =
      ValuesExtractor.extractAllFields(map, ignored.map(_.value.last))

    override def field(path: Path): Option[Any] = Option(ValuesExtractor.extract(map, path.value))
  }

  private case class KeyValueRecord(key: ParsedSinkRecord, value: ParsedSinkRecord) extends ParsedKeyValueSinkRecord {
    override def valueFields(ignored: Set[Path]): Seq[(String, Any)] = value.valueFields(ignored)

    override def field(path: Path): Option[Any] = path.value.headOption match {
      case Some(fieldName) if Util.caseInsensitiveComparison(fieldName, Util.KEY_CONSTANT) =>
        key.field(Path(path.value.tail))
      case Some(_) => value.field(path)
      case None    => throw new IllegalArgumentException("Unreachable situation detected. Path should never be empty")
    }

    override def keyFields(ignored: Set[Path]): Seq[(String, Any)] = key.valueFields(ignored)
  }

  def build(record: SinkRecord): Try[ParsedKeyValueSinkRecord] = {

    val key = Option(record.keySchema()).map(_.`type`()) match {
      case Some(Schema.Type.STRING) => Try(JsonSinkRecord(JacksonJson.asJson(record.key().asInstanceOf[String])))
      case Some(Schema.Type.STRUCT) => Try(StructSinkRecord(record.key().asInstanceOf[Struct]))
      case Some(other)              => throw new IllegalStateException(s"Unexpected $other")
      case None                     => Try(MapSinkRecord(record.key().asInstanceOf[java.util.Map[String, Any]]))
    }

    val value = Option(record.valueSchema()).map(_.`type`()) match {
      case Some(Schema.Type.STRING) =>
        Try(
          require(record.value() != null && record.value().getClass == classOf[String],
                  "The SinkRecord payload should be of type String",
          ),
        ).flatMap(_ => Try(JsonSinkRecord(JacksonJson.asJson(record.value().asInstanceOf[String]))))
      case Some(Schema.Type.STRUCT) =>
        Try(
          require(record.value() != null && record.value().getClass == classOf[Struct],
                  "The SinkRecord payload should be of type Struct",
          ),
        ).flatMap(_ => Try(StructSinkRecord(record.value().asInstanceOf[Struct])))
      case Some(other) =>
        throw new IllegalStateException(s"unexpected match for $other")
      case None =>
        Try(
          require(
            record.value() != null && record.value().isInstanceOf[java.util.Map[_, _]],
            "The SinkRecord payload should be of type java.util.Map[String, Any]",
          ),
        ).flatMap(_ => Try(MapSinkRecord(record.value().asInstanceOf[java.util.Map[String, Any]])))
    }

    key
      .flatMap(key => value.map(key -> _))
      .map { case (k, v) => KeyValueRecord(k, v) }
  }
}
