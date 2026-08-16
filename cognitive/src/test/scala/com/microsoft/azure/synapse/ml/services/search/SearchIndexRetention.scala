// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.services.search

import java.time.format.{DateTimeFormatter, DateTimeFormatterBuilder, DateTimeParseException, SignStyle}
import java.time.temporal.ChronoField
import java.time.{Duration, LocalDateTime}
import scala.util.matching.Regex

/** Decides which of the shared search service's test indexes a run is allowed to delete.
  *
  * The service will only hold [[MaxIndexes]] indexes and the search suites create one per test, so
  * a run that deletes only what it created will eventually find the service full and fail in
  * `beforeAll` before it can create anything. Collecting purely by age does not save it either
  * once the cap is reached faster than the retention window expires. That is what happened in
  * issue #2639: the service sat at 48 of 50 indexes while the oldest was barely 40 hours old, so a
  * two day cutoff selected nothing and every build in the repository, `master` included, failed
  * until the indexes were deleted by hand.
  *
  * The policy here is therefore driven by pressure rather than by age alone. Anything past
  * [[RoutineAge]] is collected on every sweep, and when that still does not leave
  * [[DesiredFreeSlots]] free the sweep keeps taking the next oldest index until it does.
  * [[MinimumAge]] is the floor that stops it from ever touching an index a concurrently running
  * build might still be using.
  */
object SearchIndexRetention {

  /** Maximum number of indexes the shared service will hold. */
  val MaxIndexes: Int = 50

  /** Slots a run wants free before it starts creating indexes of its own. */
  val DesiredFreeSlots: Int = 10

  /** Never collect an index younger than this. A full pipeline run finishes well inside an hour,
    * so an index older than this cannot still belong to a build that is running.
    *
    * This holds only while every run stamps names on the same clock. [[age]] can do no better
    * than subtract the name's timestamp from the caller's `now`, so a name written in local time
    * and read from another zone is wrong by the offset between them, and an offset larger than
    * this floor would let a sweep collect an index a live build had just created. Producers and
    * callers therefore both work in UTC.
    */
  val MinimumAge: Duration = Duration.ofHours(3)

  /** Collected on every sweep, however much room happens to be left. */
  val RoutineAge: Duration = Duration.ofHours(12)

  /** Test index names are the ones `generateIndexName` writes: a `test-` prefix, then a hash, then
    * the 17 digit timestamp [[Formatter]] produces.
    *
    * The prefix is part of the match on purpose. The sweep reads every index in the shared
    * service, not just this suite's, so matching on the timestamp alone would let it delete a
    * foreign index that happened to end in 17 digits. That was survivable while collection only
    * touched indexes older than two days; now that a sweep routinely collects at [[RoutineAge]]
    * and drops to [[MinimumAge]] under pressure, it is not.
    */
  private val TimestampedName: Regex = "^test-.*-(\\d{17})$".r

  /** When a date pattern starts with 'yyyy' and has no separator following, the parser can
    * sometimes decide to take the whole string to match the year, which results in an exception.
    * The following is a hackaround.
    */
  val Formatter: DateTimeFormatter = new DateTimeFormatterBuilder()
    .appendValue(ChronoField.YEAR_OF_ERA, 4, 4, SignStyle.EXCEEDS_PAD)
    .appendPattern("MMddHHmmssSSS").toFormatter()

  /** How old the index its name says it is, or None when the name carries no usable timestamp. */
  def age(name: String, now: LocalDateTime): Option[Duration] = name match {
    case TimestampedName(stamp) =>
      try Some(Duration.between(LocalDateTime.parse(stamp, Formatter), now))
      catch { case _: DateTimeParseException => None }
    case _ => None
  }

  /** Every index this run is allowed to delete, oldest first.
    *
    * An index whose name carries no timestamp is never collected: without an age there is no way
    * to tell it apart from one a running build just created.
    */
  def collectable(existing: Seq[String], now: LocalDateTime): Seq[(String, Duration)] =
    existing.flatMap(name => age(name, now).map(name -> _))
      .filter { case (_, indexAge) => indexAge.compareTo(MinimumAge) > 0 }
      .sortBy { case (_, indexAge) => -indexAge.toMillis }

  /** The indexes to delete on this sweep, oldest first. */
  def select(existing: Seq[String], now: LocalDateTime): Seq[String] = {
    val candidates = collectable(existing, now)
    // candidates is oldest first, so the routine ones are exactly its first `routine` entries and
    // taking any more than that walks steadily towards the youngest index still safe to collect.
    val routine = candidates.count { case (_, indexAge) => indexAge.compareTo(RoutineAge) > 0 }
    val freeAfterRoutine = MaxIndexes - existing.size + routine
    val shortfall = math.max(0, DesiredFreeSlots - freeAfterRoutine)
    candidates.take(routine + shortfall).map { case (name, _) => name }
  }
}
