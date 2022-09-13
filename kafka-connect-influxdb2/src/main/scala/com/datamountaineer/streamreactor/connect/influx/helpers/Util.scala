package com.datamountaineer.streamreactor.connect.influx2.helpers

import scala.util.Failure
import scala.util.Success
import scala.util.Try

object Util {

  def shortCircuitOnFailure[T](a: Try[Seq[T]], b: Try[T]): Try[Seq[T]] = (a, b) match {
    case (Failure(e), _)              => Failure(e)
    case (Success(r), Success(value)) => Success(r :+ value)
    case (_, Failure(e))              => Failure(e)
  }

  def caseInsensitiveComparison(a: String, b: String): Boolean = a.toUpperCase == b.toUpperCase

  def caseInsensitiveComparison(a: Seq[String], b: Seq[String]): Boolean = a.map(_.toUpperCase) == b.map(_.toUpperCase)

  val KEY_CONSTANT     = "_key"
  val KEY_All_ELEMENTS = Vector(KEY_CONSTANT, "*")

}
