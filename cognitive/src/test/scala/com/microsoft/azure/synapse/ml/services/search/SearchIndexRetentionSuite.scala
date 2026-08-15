// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.services.search

import com.microsoft.azure.synapse.ml.core.test.base.TestBase

import java.time.LocalDateTime

/** Covers the retention policy that keeps the shared search service from filling up (issue #2639).
  *
  * None of this talks to the service, so it runs on every build rather than only on the ones that
  * have search credentials, which matters because the bug it guards against took down `master`.
  */
class SearchIndexRetentionSuite extends TestBase {

  import SearchIndexRetention._

  private val now: LocalDateTime = LocalDateTime.of(2026, 8, 15, 12, 0, 0)

  /** A name shaped exactly like the ones generateIndexName produces. */
  private def index(hoursOld: Long, tag: String = "1534278552"): String =
    s"test-$tag-${Formatter.format(now.minusHours(hoursOld))}"

  /** Enough young indexes to sit `holding` short of the cap without any being collectable. */
  private def young(count: Int): Seq[String] =
    (1 to count).map(i => index(1, s"young$i"))

  test("An index younger than the minimum age is never collected") {
    val recent = Seq(index(MinimumAge.toHours - 1))
    assert(collectable(recent, now).isEmpty)
    assert(select(recent, now).isEmpty)
  }

  test("An index past the routine age is collected even when the service is nearly empty") {
    val old = index(RoutineAge.toHours + 1)
    assert(select(Seq(old), now) == Seq(old))
  }

  test("An index between the minimum and routine ages is kept while there is room") {
    val middling = index(RoutineAge.toHours - 1)
    assert(collectable(Seq(middling), now).map { case (n, _) => n } == Seq(middling))
    assert(select(Seq(middling), now).isEmpty)
  }

  test("Pressure reclaims indexes that are too young for the routine sweep") {
    // This is issue #2639: the service one index below its cap with nothing past the old two day
    // cutoff. A purely age based sweep selected nothing here and the suite then failed to create.
    val reclaimable = (1 to 20).map(i => index(MinimumAge.toHours + i, s"mid$i"))
    val existing = reclaimable ++ young(MaxIndexes - 2 - reclaimable.size)
    assert(existing.size == MaxIndexes - 2)
    val collected = select(existing, now)
    assert(collected.nonEmpty)
    assert(existing.size - collected.size <= MaxIndexes - DesiredFreeSlots)
  }

  test("Pressure reclaims the oldest indexes first") {
    val existing = (1 to 20).map(i => index(MinimumAge.toHours + i, s"mid$i")) ++
      young(MaxIndexes - 2 - 20)
    val collected = select(existing, now)
    val ages = collected.map(name => age(name, now).get.toHours)
    assert(ages == ages.sorted.reverse, "expected oldest first")
    assert(ages.min > MinimumAge.toHours, "must not cross the safety floor")
  }

  test("Pressure never collects an index a running build could still own") {
    // Every index is under the floor, so even at the cap there is nothing safe to take.
    val existing = (1 to MaxIndexes).map(i => index(MinimumAge.toHours - 1, s"busy$i"))
    assert(select(existing, now).isEmpty)
  }

  test("The pressure sweep stops once it has freed enough room") {
    // Every index sits between the two ages, so nothing is collected routinely and the whole
    // selection is pressure driven. It should stop the moment there is enough room, not keep going.
    val existing = (1 to MaxIndexes).map(i =>
      index(MinimumAge.toHours + 1 + (i % (RoutineAge.toHours - MinimumAge.toHours - 1)), s"mid$i"))
    assert(collectable(existing, now).size == MaxIndexes, "expected every index to be collectable")
    assert(select(existing, now).size == DesiredFreeSlots)
  }

  test("The routine sweep is not capped by the free slot target") {
    // Anything past the routine age is garbage, so it all goes even though far fewer would do.
    val existing = (1 to MaxIndexes).map(i => index(RoutineAge.toHours + i, s"old$i"))
    assert(select(existing, now).size == MaxIndexes)
  }

  test("An index with no timestamp in its name is left alone") {
    // Nothing can be inferred about its age, so it is not this sweep's to delete.
    val untimestamped = Seq("test-website", "test-33467690", "examplevectorindex")
    assert(select(untimestamped, now).isEmpty)
    assert(untimestamped.flatMap(age(_, now)).isEmpty)
  }

  test("An index whose timestamp is not a real date is left alone") {
    assert(age("test-1-99999999999999999", now).isEmpty)
    assert(select(Seq("test-1-99999999999999999"), now).isEmpty)
  }

  test("Age is read back from the name generateIndexName would have written") {
    assert(age(index(5), now).map(_.toHours).contains(5L))
  }

  test("The safety floor leaves room for a pipeline run to finish") {
    // A full run is well under an hour; anything near that would make the sweep race live builds.
    assert(MinimumAge.toHours >= 2)
    assert(RoutineAge.compareTo(MinimumAge) > 0)
    assert(DesiredFreeSlots > 0 && DesiredFreeSlots < MaxIndexes)
  }
}
