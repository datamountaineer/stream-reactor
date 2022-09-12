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

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node._
import org.apache.kafka.connect.data._

import scala.annotation.tailrec
import scala.jdk.CollectionConverters.IteratorHasAsScala
import scala.jdk.CollectionConverters.ListHasAsScala
import scala.jdk.CollectionConverters.MapHasAsScala

object ValuesExtractor {

  /**
    * Extracts all the keys and values from the source json. It will ignore the fields present in the ignored collection
    *
    * @param node    -  The source json to extract all the key and values. Only ObjectNode is accepted - those are the ones accepting fields
    * @param ignored - A collection of keys to ignore
    * @return
    * The list of keys and values. Throws [[IllegalArgumentException]] if any of the values are not primitives
    */
  def extractAllFields(node: JsonNode, ignored: Set[String]): Seq[(String, Any)] =
    node match {
      case o: ObjectNode =>
        o.fields().asScala.filter(p => !ignored.contains(p.getKey))
          .map { kvp =>
            val value = kvp.getValue match {
              case b: BooleanNode    => b.booleanValue()
              case i: BigIntegerNode => i.bigIntegerValue()
              case d: DecimalNode    => d.decimalValue()
              case d: DoubleNode     => d.doubleValue()
              case f: FloatNode      => f.floatValue()
              case i: IntNode        => i.intValue()
              case l: LongNode       => l.longValue()
              case s: ShortNode      => s.shortValue()
              case t: TextNode       => t.textValue()
              case _: NullNode       => null
              case _: MissingNode    => null
              case other =>
                throw new IllegalArgumentException(
                  s"You can't select all fields from the Kafka message because ${kvp.getKey} resolves to a complex type:${Option(other).map(_.getClass.getCanonicalName).orNull})",
                )
            }
            kvp.getKey -> value

          }.toList
      case other =>
        throw new IllegalArgumentException(
          s"You can't select all fields from the Kafka message because the incoming message resolves to a not allowed type:${Option(other).map(_.getClass.getCanonicalName).orNull})",
        )
    }

  /**
    * Extracts the value for the given field path from the specified Json
    *
    * @param node      - The Json to extract the field value
    * @param fieldPath - The path to the field to get the value for.
    * @return
    */
  def extract(node: JsonNode, fieldPath: Seq[String]): Any = {
    @tailrec
    def innerExtract(n: JsonNode, path: Seq[String]): Any = {
      def checkValidPath() =
        if (path.nonEmpty) {
          throw new IllegalArgumentException(
            s"Invalid field selection for '${fieldPath.mkString(".")}'. It doesn't resolve to a primitive field",
          )
        }

      n match {
        case _: BinaryNode =>
          throw new IllegalArgumentException(
            s"Invalid field selection for '${fieldPath.mkString(".")}'. The path is resolving to a binary node and InfluxDB API doesn't support binary fields for a Point.",
          )
        case _: BooleanNode =>
          checkValidPath()
          n.booleanValue()
        case _: BigIntegerNode =>
          checkValidPath()
          n.bigIntegerValue()
        case _: DecimalNode =>
          checkValidPath()
          n.decimalValue()
        case _: DoubleNode =>
          checkValidPath()
          n.doubleValue()
        case _: FloatNode =>
          checkValidPath()
          n.floatValue()
        case _: IntNode =>
          checkValidPath()
          n.intValue()
        case _: LongNode =>
          checkValidPath()
          n.longValue()
        case _: ShortNode =>
          checkValidPath()
          n.shortValue()
        case _: TextNode =>
          checkValidPath()
          n.textValue()
        case _: NullNode =>
          checkValidPath()
          null
        case _: MissingNode =>
          checkValidPath()
          null

        case node: ObjectNode =>
          if (path.isEmpty) {
            throw new IllegalArgumentException(
              s"Invalid field selection for '${fieldPath.mkString(".")}'. The path is not resolving to a primitive field",
            )
          }
          val childNode = node.get(path.head)
          if (childNode == null) {
            null
          } else {

            innerExtract(childNode, path.tail)
          }
        case _: ArrayNode =>
          throw new IllegalArgumentException(
            s"Invalid field selection for '${fieldPath.mkString(".")}'. The path is resolving to an array",
          )

        case other =>
          throw new IllegalArgumentException(
            s"Invalid field selection for '${fieldPath.mkString(".")}'. $other is not handled",
          )
      }
    }

    if (node == null) {
      throw new NullPointerException("Invalid parameter 'node'")
    }
    innerExtract(node, fieldPath)
  }

  /**
    * Extracts all the fields and values from the source struct. It will ignore the fields present in the ignored collection
    *
    * @param struct  -  An instance of Kafka Connect [[Struct]] json to extract all the fields and values from. If the fields resolve to Struct/Map/Array and error is thrown
    * @param ignored - A collection of keys to ignore
    * @return
    * The list of keys and values. Throws [[IllegalArgumentException]] if any of the values are not primitives
    */
  def extractAllFields(struct: Struct, ignored: Set[String]): Seq[(String, Any)] =
    struct.schema().fields.asScala.toList
      .filter(f => !ignored.contains(f.name()))
      .map { field =>
        val value = struct.get(field)
        val actualValue = Option(field.schema().name()).collect {
          case Decimal.LOGICAL_NAME =>
            value match {
              case bd: BigDecimal  => bd
              case _:  Array[Byte] => Decimal.toLogical(field.schema, value.asInstanceOf[Array[Byte]])

            }
          case Date.LOGICAL_NAME =>
            value.asInstanceOf[Any] match {
              case d: java.util.Date => d
              case i: Int            => Date.toLogical(field.schema, i)
              case _ =>
                throw new IllegalArgumentException(
                  s"Can't convert $value to Date for schema:${field.schema().`type`()}",
                )
            }
          case Time.LOGICAL_NAME =>
            value.asInstanceOf[Any] match {
              case _: Int            => Time.toLogical(field.schema, value.asInstanceOf[Int])
              case d: java.util.Date => d
              case _ =>
                throw new IllegalArgumentException(
                  s"Can't convert $value to Date for schema:${field.schema().`type`()}",
                )
            }
          case Timestamp.LOGICAL_NAME =>
            value.asInstanceOf[Any] match {
              case l: Long           => Timestamp.toLogical(field.schema, l)
              case d: java.util.Date => d
              case _ =>
                throw new IllegalArgumentException(
                  s"Can't convert $value to Date for schema:${field.schema().`type`()}",
                )
            }
        }.getOrElse {
          field.schema().`type`() match {
            case Schema.Type.BOOLEAN | Schema.Type.BYTES | Schema.Type.FLOAT32 | Schema.Type.FLOAT64 |
                Schema.Type.INT8 | Schema.Type.INT16 | Schema.Type.INT32 | Schema.Type.INT64 | Schema.Type.STRING |
                Schema.Type.ARRAY => value

            case other =>
              throw new IllegalArgumentException(
                s"You can't select * from the Kafka Message. Field:'${field.name()}' resolves to a Schema '$other' which will end up with a type not supported by InfluxDB API.",
              )

          }
        }
        field.name() -> actualValue
      }

  /**
    * Extracts the value for the given field path from the a Kafka Connect Struct
    *
    * @param struct    - The Kafka Connect Struct to extract the field value from
    * @param fieldPath - The path to the field to get the value for.
    * @return
    */
  def extract(struct: Struct, fieldPath: Seq[String]): Any = {
    // @tailrec
    def innerExtract(field: Field, value: AnyRef, path: Seq[String]): Any = {
      def checkValidPath() =
        if (path.nonEmpty) {
          throw new IllegalArgumentException(
            s"Invalid field selection for '${fieldPath.mkString(".")}'. It doesn't resolve to a primitive field",
          )
        }

      if (value == null) {
        null
      } else {
        Option(field.schema().name()).collect {
          case Decimal.LOGICAL_NAME =>
            value match {
              case bd: BigDecimal =>
                checkValidPath()
                bd
              case bd: java.math.BigDecimal =>
                checkValidPath()
                bd
              case _: Array[Byte] =>
                checkValidPath()
                Decimal.toLogical(field.schema, value.asInstanceOf[Array[Byte]])
            }
          case Date.LOGICAL_NAME =>
            value.asInstanceOf[Any] match {
              case d: java.util.Date =>
                checkValidPath()
                d
              case i: Int =>
                checkValidPath()
                Date.toLogical(field.schema, i)
              case _ =>
                throw new IllegalArgumentException(
                  s"Can't convert $value to Date for schema:${field.schema().`type`()}",
                )
            }
          case Time.LOGICAL_NAME =>
            value.asInstanceOf[Any] match {
              case _: Int =>
                checkValidPath()
                Time.toLogical(field.schema, value.asInstanceOf[Int])
              case d: java.util.Date =>
                checkValidPath()
                d
              case _ =>
                throw new IllegalArgumentException(
                  s"Can't convert $value to Date for schema:${field.schema().`type`()}",
                )
            }
          case Timestamp.LOGICAL_NAME =>
            value.asInstanceOf[Any] match {
              case l: Long =>
                checkValidPath()
                Timestamp.toLogical(field.schema, l)
              case d: java.util.Date =>
                checkValidPath()
                d
              case _ =>
                throw new IllegalArgumentException(
                  s"Can't convert $value to Date for schema:${field.schema().`type`()}",
                )
            }
        }.getOrElse {
          val v = field.schema().`type`() match {
            case Schema.Type.BOOLEAN =>
              checkValidPath()
              value.asInstanceOf[Boolean]
            case Schema.Type.BYTES =>
              checkValidPath()
              value.asInstanceOf[Array[Byte]]
            case Schema.Type.FLOAT32 =>
              checkValidPath()
              value.asInstanceOf[Float]
            case Schema.Type.FLOAT64 =>
              checkValidPath()
              value.asInstanceOf[Double]
            case Schema.Type.INT8 =>
              checkValidPath()
              value.asInstanceOf[Byte]
            case Schema.Type.INT16 =>
              checkValidPath()
              value.asInstanceOf[Short]
            case Schema.Type.INT32 =>
              checkValidPath()
              value.asInstanceOf[Int]
            case Schema.Type.INT64 =>
              checkValidPath()
              value.asInstanceOf[Long]
            case Schema.Type.STRING =>
              checkValidPath()
              value.toString

            case Schema.Type.MAP =>
              if (path.isEmpty) {
                throw new IllegalArgumentException(
                  s"Invalid field selection for '${fieldPath.mkString(".")}'. It doesn't resolve to a primitive field. It resolves to:${field.schema()}",
                )
              }
              val map = value.asInstanceOf[java.util.Map[String, AnyRef]]
              val f   = new Field(path.head, 0, field.schema().valueSchema())

              innerExtract(f, map.get(path.head), path.tail)

            case Schema.Type.STRUCT =>
              if (path.isEmpty) {
                throw new IllegalArgumentException(
                  s"Invalid field selection for '${fieldPath.mkString(".")}'. It doesn't resolve to a primitive field. It resolves to:${field.schema()}",
                )
              }
              val s = value.asInstanceOf[Struct]
              val childField = Option(s.schema().field(path.head))
                .getOrElse {
                  throw new IllegalArgumentException(
                    s"Invalid field selection for '${fieldPath.mkString(
                      ".",
                    )}'. Can't find field '${path.head}'. Fields available:${s.schema().fields().asScala.map(_.name()).mkString(",")}",
                  )
                }

              innerExtract(childField, s.get(childField), path.tail)
            case other =>
              throw new IllegalArgumentException(
                s"$other is not a supported schema. It's value will revolve to types not supported by InfluxDb API",
              )
          }
          v
        }
      }
    }

    val field = Option(struct.schema().field(fieldPath.head)).getOrElse {
      throw new IllegalArgumentException(
        s"Couldn't find field '${fieldPath.head}' in the schema:${struct.schema().fields().asScala.map(_.name()).mkString(",")}",
      )
    }

    innerExtract(field, struct.get(field), fieldPath.tail)
  }

  /**
    * Extracts all the keys and values from the input map. It will ignore the keys present in the ignored collection
    *
    * @param map     -  The source map to extract all the key and values
    * @param ignored - A collection of keys to ignore
    * @return
    * The list of keys and values. Throws [[IllegalArgumentException]] if any of the values are not primitives
    */
  def extractAllFields(map: java.util.Map[String, Any], ignored: Set[String]): Seq[(String, Any)] =
    map.asScala.filter(p => !ignored.contains(p._1) && p._2 != null)
      .map {
        case (k, value) =>
          value match {
            case _: Long | _: Int | _: BigInt | _: Byte | _: Short | _: Double | _: Float | _: Boolean |
                _: java.math.BigDecimal | _: String | _: BigDecimal => k -> value

            case other =>
              throw new IllegalArgumentException(
                s"You can't select all the fields because '$k' is resolving to a type: '${other.getClass.getCanonicalName}' which is not supported by InfluxDB API",
              )
          }
      }.toSeq

  /**
    * Extracts the value for the given field path from the specified map.
    *
    * @param map       - The map to extract the field value
    * @param fieldPath - The path to the field to get the value for.
    * @return
    */
  def extract(map: java.util.Map[String, Any], fieldPath: Seq[String]): Any = {
    @tailrec
    def innerExtract(value: Any, path: Seq[String]): Any = {
      def checkValidPath() =
        if (path.nonEmpty) {
          throw new IllegalArgumentException(
            s"Invalid field selection for '${fieldPath.mkString(".")}'. It doesn't resolve to a primitive field",
          )
        }

      value match {
        case m: java.util.Map[_, _] =>
          if (path.isEmpty) {
            throw new IllegalArgumentException(
              s"Invalid field selection for '${fieldPath.mkString(".")}'. The path is not resolving to a primitive type",
            )
          }
          innerExtract(m.asInstanceOf[java.util.Map[String, Any]].get(path.head), path.tail)

        case _: java.util.Collection[_] =>
          throw new IllegalArgumentException(
            s"Invalid field selection for '${fieldPath.mkString(".")}'. The path is not resolving to a primitive type. It resolves to a Collection.",
          )

        case _: Array[_] =>
          throw new IllegalArgumentException(
            s"Invalid field selection for '${fieldPath.mkString(".")}'. The path is not resolving to a primitive type. It resovles to an Array.",
          )

        case other =>
          checkValidPath()
          other
      }
    }

    if (map == null) {
      throw new NullPointerException("Invalid parameter 'map'")
    }
    innerExtract(map, fieldPath)
  }

}
