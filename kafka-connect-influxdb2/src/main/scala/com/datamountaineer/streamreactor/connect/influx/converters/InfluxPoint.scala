package com.datamountaineer.streamreactor.connect.influx2.converters

import com.datamountaineer.streamreactor.connect.influx2.NanoClock
import com.datamountaineer.streamreactor.connect.influx2.converters.SinkRecordParser.{ParsedKeyValueSinkRecord, ParsedSinkRecord}
import com.datamountaineer.streamreactor.connect.influx2.helpers.Util
import com.datamountaineer.streamreactor.connect.influx2.writers.KcqlDetails
import com.datamountaineer.streamreactor.connect.influx2.writers.KcqlDetails.{ConstantTag, DynamicTag, Path}
import com.influxdb.client.write.Point

import java.time.Instant
import java.util.concurrent.TimeUnit
import java.util.Date
import com.influxdb.client.domain.WritePrecision
import scala.jdk.CollectionConverters.ListHasAsScala
import scala.util.{Failure, Success, Try}

object InfluxPoint {

    val T_to_I = Map(
        TimeUnit.DAYS -> WritePrecision.S,
        TimeUnit.HOURS -> WritePrecision.S,
        TimeUnit.MINUTES -> WritePrecision.S,
        TimeUnit.SECONDS -> WritePrecision.S,
        TimeUnit.MILLISECONDS -> WritePrecision.MS,
        TimeUnit.MICROSECONDS -> WritePrecision.US,
        TimeUnit.NANOSECONDS -> WritePrecision.NS
    )


  def build(nanoClock: NanoClock)(record: ParsedKeyValueSinkRecord,
                                  details: KcqlDetails): Try[Point] =
    for {
      (timeUnit, timestamp) <- extractTimeMeasures(nanoClock, record, details)
      measurement = details.dynamicTarget
        .flatMap(record.field)
        .map(_.toString)
        .getOrElse(details.target)
      infl_timeunit = T_to_I(timeUnit)
      pointBuilder = Point.measurement(measurement).time(timestamp, infl_timeunit)
      point <- addValuesAndTags(pointBuilder, record, details)
    } yield point.build()

  private def addValuesAndTags(pointBuilder: Point.Builder,
                               record: ParsedKeyValueSinkRecord,
                               details: KcqlDetails): Try[Point.Builder] =
    details.NonIgnoredFields
      .flatMap {
        case (_, path, _) if path.equals(Path(Util.KEY_All_ELEMENTS)) =>
          record.keyFields(ignored = details.IgnoredAndAliasFields).map {
            case (name, value) => name -> Some(value)
          }
        case (field, _, _) if field.value == "*" =>
          record.valueFields(ignored = details.IgnoredAndAliasFields).map {
            case (name, value) => name -> Some(value)
          }
        case (field, path, _) => (field.value -> record.field(path)) :: Nil
      }
      .foldLeft(Try(pointBuilder))((builder, field) =>
        builder.flatMap(b =>
          field match {
            case (key, Some(value)) => writeField(b)(key, value)
            case (key, _) =>
              Failure(
                new IllegalArgumentException(
                  s"Property $key is referenced but no value could be found ."))
        }))
      .flatMap(builder =>
        details.tags
          .map {
            case ConstantTag(key, value) => (key.value, Some(value))
            case DynamicTag(name, path) =>
              (name.value, record.field(path).map(_.toString))
          }
          .foldLeft(Try(builder))((builder, tag) => {
            tag match {
              case (key, Some(value)) => builder.map(_.tag(key, value))
              case (_, None)          => builder
            }
          }))

  private def extractTimeMeasures(nanoClock: NanoClock,
                                  record: ParsedSinkRecord,
                                  details: KcqlDetails): Try[(TimeUnit, Long)] =
    details.timestampField
      .flatMap { path =>
        record
          .field(path)
          .map(coerceTimeStamp(_, path.value).map(details.timestampUnit -> _))
      }
      .getOrElse(Try(TimeUnit.NANOSECONDS -> nanoClock.getEpochNanos))

  private[influx2] def coerceTimeStamp(
      value: Any,
      fieldPath: Iterable[String]): Try[Long] = {
    value match {
      case b: Byte  => Try(b.toLong)
      case s: Short => Try(s.toLong)
      case i: Int   => Try(i.toLong)
      case l: Long  => Try(l)
      case s: String =>
        Try(Instant.parse(s).toEpochMilli).transform(
          Try(_),
          e =>
            Failure(
              new IllegalArgumentException(
                s"$s is not a valid format for timestamp, expected 'yyyy-MM-DDTHH:mm:ss.SSSZ'",
                e)))
      case d: Date => Try(d.toInstant.toEpochMilli)
      /* Assume Unix timestamps in seconds with double precision, coerce to Long with microseconds precision */
      case d: Double => Try((d * 1E6).toLong)
      case other =>
        Failure(new IllegalArgumentException(
          s"Invalid value for field:${fieldPath.mkString(".")}.Value '$other' is not a valid field for the timestamp"))
    }
  }

  def writeField(builder: Point.Builder)(field: String,
                                         v: Any): Try[Point.Builder] = v match {
    case value: Long                 => Try(builder.addField(field, value))
    case value: Int                  => Try(builder.addField(field, value))
    case value: BigInt               => Try(builder.addField(field, value))
    case value: Byte                 => Try(builder.addField(field, value.toShort))
    case value: Short                => Try(builder.addField(field, value))
    case value: Double               => Try(builder.addField(field, value))
    case value: Float                => Try(builder.addField(field, value))
    case value: Boolean              => Try(builder.addField(field, value))
    case value: java.math.BigDecimal => Try(builder.addField(field, value))
    case value: BigDecimal           => Try(builder.addField(field, value.bigDecimal))
    case value: String               => Try(builder.addField(field, value))
    case value: java.util.Date       => Try(builder.addField(field, value.getTime))
    case value: java.util.List[_]    => flattenArray(builder)(field, value)
    case value =>
      Failure(new RuntimeException(
        s"Can't select field:'$field' because it leads to value:'$value' (${Option(
          value).map(_.getClass.getName).getOrElse("")})is not a valid type for InfluxDb."))
  }

  /**
   * Flatten an array writing each element as a new field with the following convention:
   * "name": ["a", "b", "c"] => name0 = "a", name1 = "b", name3 = "c"
   */
  private def flattenArray(builder: Point.Builder)(field: String, value: java.util.List[_]) = {
    val res = value.asScala.zipWithIndex.map {
      case (el, i) => writeField(builder)(field + i, el)
    }
    // return first failure
    res.collectFirst({
      case Failure(x) => Failure(x)
    }).getOrElse(Success(builder))
  }
}
