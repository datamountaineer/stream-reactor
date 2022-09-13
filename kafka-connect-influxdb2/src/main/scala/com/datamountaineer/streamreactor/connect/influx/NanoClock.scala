package com.datamountaineer.streamreactor.connect.influx2

import java.time.temporal.ChronoField
import java.time.Clock
import java.time.Instant
import java.time.ZoneId

/**
  * Trying to work against JVM time precision where Nano support is not provided until Java 9.
  * @param clock
  */
class NanoClock(val clock: Clock) {
  private val initialNanos = getSystemNanos
  private val initialInstant: Instant = clock.instant()

  def this() = {
    this(Clock.systemUTC)
  }

  def getZone = clock.getZone

  def instant = initialInstant.plusNanos(getSystemNanos - initialNanos)

  def withZone(zone: ZoneId) = new NanoClock(clock.withZone(zone))

  def getEpochNanos: Long = {
    val now = instant
    now.toEpochMilli * 1000000L + (now.getLong(ChronoField.NANO_OF_SECOND) % 1000000)
  }
  private def getSystemNanos = System.nanoTime
}
