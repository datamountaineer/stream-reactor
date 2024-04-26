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
package io.lenses.streamreactor.connect.elastic.common.transform

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node._
import org.apache.kafka.connect.data._
import org.apache.kafka.connect.errors.ConnectException

import scala.annotation.tailrec
import scala.jdk.CollectionConverters.IteratorHasAsScala
import scala.jdk.CollectionConverters.ListHasAsScala

object PrimaryKeyExtractor {
  def extract(node: JsonNode, path: Vector[String]): Any = {
    @tailrec
    def innerExtract(n: JsonNode, p: Vector[String]): Any = {
      def checkValidPath(): Unit =
        if (p.nonEmpty) {
          throw new IllegalArgumentException(
            s"Invalid field selection for '${path.mkString(".")}'. It doesn't resolve to a primitive field",
          )
        }

      n match {
        case null => null
        case _: BinaryNode =>
          checkValidPath()
          n.binaryValue()

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
          if (p.isEmpty) {
            throw new IllegalArgumentException(
              s"Invalid field selection for '${path.mkString(".")}'. The path is not resolving to a primitive field",
            )
          }
          val childNode = Option(node.get(p.head)).getOrElse {
            throw new IllegalArgumentException(
              s"Invalid field selection for '${path.mkString(".")}'. Can't find ${p.head} field. Field found are:${node.fieldNames().asScala.mkString(",")}",
            )
          }

          innerExtract(childNode, p.tail)
        case _: ArrayNode =>
          throw new IllegalArgumentException(
            s"Invalid field selection for '${path.mkString(".")}'. The path is involving an array structure",
          )

        case other =>
          throw new IllegalArgumentException(
            s"Invalid field selection for '${path.mkString(".")}'. $other is not handled",
          )
      }
    }

    if (node == null) {
      throw new NullPointerException("Invalid parameter 'node'")
    }
    innerExtract(node, path)
  }

  def extract(struct: Struct, path: Vector[String]): Any = {
    // @tailrec
    def innerExtract(field: Field, value: AnyRef, p: Vector[String]): Any = {
      def checkValidPath() =
        if (p.nonEmpty) {
          throw new IllegalArgumentException(
            s"Invalid field selection for '${path.mkString(".")}'. It doesn't resolve to a primitive field",
          )
        }

      if (value == null) {
        throw new IllegalArgumentException(
          s"Invalid field selection for '${path.mkString(".")}'. Field '${field.name()}' is null",
        )
      }
      Option(field.schema().name()).collect {
        case Decimal.LOGICAL_NAME =>
          value match {
            case bd: BigDecimal =>
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
              throw new IllegalArgumentException(s"Can't convert $value to Date for schema:${field.schema().`type`()}")
          }
        case Time.LOGICAL_NAME =>
          value.asInstanceOf[Any] match {
            case i: Int =>
              checkValidPath()
              Time.toLogical(field.schema, i)
            case d: java.util.Date =>
              checkValidPath()
              d
            case _ =>
              throw new IllegalArgumentException(s"Can't convert $value to Date for schema:${field.schema().`type`()}")
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
              throw new IllegalArgumentException(s"Can't convert $value to Date for schema:${field.schema().`type`()}")
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
            if (p.isEmpty) {
              throw new IllegalArgumentException(
                s"Invalid field selection for '${path.mkString(".")}'. It doesn't resolve to a primitive field. It resolves to:${field.schema()}",
              )
            }
            val map = value.asInstanceOf[java.util.Map[String, AnyRef]]
            val f   = new Field(p.head, 0, field.schema().valueSchema())

            innerExtract(f, map.get(p.head), p.tail)

          case Schema.Type.STRUCT =>
            if (p.isEmpty) {
              throw new IllegalArgumentException(
                s"Invalid field selection for '${path.mkString(".")}'. It doesn't resolve to a primitive field. It resolves to:${field.schema()}",
              )
            }
            val s = value.asInstanceOf[Struct]
            val childField = Option(s.schema().field(p.head))
              .getOrElse {
                throw new IllegalArgumentException(s"Invalid field selection for '${path.mkString(
                  ".",
                )}'. Can't find field '${p.head}'. Fields available:${s.schema().fields().asScala.map(_.name()).mkString(",")}")
              }

            innerExtract(childField, s.get(childField), p.tail)
          case other => throw new ConnectException(s"$other is not a recognized schema")
        }
        v
      }
    }

    val field = Option(struct.schema().field(path.head)).getOrElse {
      throw new IllegalArgumentException(
        s"Couldn't find field '${path.head}' in the schema:${struct.schema().fields().asScala.map(_.name()).mkString(",")}",
      )
    }

    innerExtract(field, struct.get(field), path.tail)
  }
}
