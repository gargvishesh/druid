/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.talaria.indexing;

import com.google.common.collect.ImmutableMap;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

public class SuperSorterProgressSnapshotTest
{

  private SuperSorterProgressTracker superSorterProgressTracker;

  private final ImmutableMap<Integer, Long> LEVEL_TO_TOTAL_MERGERS_COMMON_MAP = ImmutableMap.<Integer, Long>builder()
                                                                                            .put(0, 16L)
                                                                                            .put(1, 8L)
                                                                                            .put(2, 4L)
                                                                                            .put(3, 2L)
                                                                                            .put(4, 1L)
                                                                                            .build();


  private final ImmutableMap<Integer, Long> LEVEL_TO_MERGED_BATCHES_COMMON_MAP = ImmutableMap.<Integer, Long>builder()
                                                                                             .put(0, 10L)
                                                                                             .put(1, 2L)
                                                                                             .put(2, 3L)
                                                                                             .put(3, 1L)
                                                                                             .put(4, 12L)
                                                                                             .build();

  private final int TOTAL_LEVELS = 5;

  // Initialize a base tracker with total mergers for levels from 0-4 set in advance and some mergers already set
  @Before
  public void setup()
  {
    superSorterProgressTracker = new SuperSorterProgressTracker();

    superSorterProgressTracker.setTotalMergersForLevel(0, 16);
    superSorterProgressTracker.setTotalMergersForLevel(1, 8);
    superSorterProgressTracker.setTotalMergersForLevel(2, 4);
    superSorterProgressTracker.setTotalMergersForLevel(3, 2);
    superSorterProgressTracker.setTotalMergersForLevel(4, 1);

    superSorterProgressTracker.addMergedBatchesForLevel(0, 5);
    superSorterProgressTracker.addMergedBatchesForLevel(0, 5);
    superSorterProgressTracker.addMergedBatchesForLevel(1, 2);
    superSorterProgressTracker.addMergedBatchesForLevel(2, 3);
    superSorterProgressTracker.addMergedBatchesForLevel(3, 1);
    superSorterProgressTracker.addMergedBatchesForLevel(4, 12);
  }

  @Test
  public void testSnapshotWhenTotalMergingLevelsUnset()
  {
    SuperSorterProgressSnapshot snapshot = superSorterProgressTracker.snapshot();

    Assert.assertNull(snapshot.getProgressDigest());
    Assert.assertEquals(-1, snapshot.getTotalMergingLevels());
    Assert.assertEquals(LEVEL_TO_TOTAL_MERGERS_COMMON_MAP, snapshot.getLevelToTotalBatches());
    Assert.assertEquals(LEVEL_TO_MERGED_BATCHES_COMMON_MAP, snapshot.getLevelToMergedBatches());
    Assert.assertEquals(false, snapshot.isTriviallyComplete());
  }

  @Test
  public void testSnapshotWhenTotalMergingLevelsSetButOutputPartitionsCountUnknown()
  {
    superSorterProgressTracker.setTotalMergingLevels(TOTAL_LEVELS);
    SuperSorterProgressSnapshot snapshot = superSorterProgressTracker.snapshot();

    Assert.assertNotNull(snapshot.getProgressDigest());

    // 10/16 + 2/8 + 3/4 + 1/2 + 0 (ultimate mergers shouldn't be taken into account if not set explicitly)
    Assert.assertEquals(0.425D, snapshot.getProgressDigest(), 1e-6);
    Assert.assertEquals(TOTAL_LEVELS, snapshot.getTotalMergingLevels());
    Map<Integer, Long> levelToTotalMergersModifiedMap = new HashMap<>(LEVEL_TO_TOTAL_MERGERS_COMMON_MAP);

    // Snapshot should replace the total levels of the ultimate in the Map if it is not set explicitly
    levelToTotalMergersModifiedMap.put(TOTAL_LEVELS - 1, -1L);
    Assert.assertEquals(levelToTotalMergersModifiedMap, snapshot.getLevelToTotalBatches());
    Assert.assertEquals(LEVEL_TO_MERGED_BATCHES_COMMON_MAP, snapshot.getLevelToMergedBatches());
    Assert.assertEquals(false, snapshot.isTriviallyComplete());
  }

  @Test
  public void testSnapshotWhenTotalMergingLevelsAndOutputPartitionsCountKnown()
  {
    superSorterProgressTracker.setTotalMergingLevels(TOTAL_LEVELS);
    superSorterProgressTracker.setTotalMergersForUltimateLevel(25);

    SuperSorterProgressSnapshot snapshot = superSorterProgressTracker.snapshot();
    Assert.assertNotNull(snapshot.getProgressDigest());

    // 10/16 + 2/8 + 3/4 + 1/2 + 12/25
    Assert.assertEquals(0.521D, snapshot.getProgressDigest(), 1e-6);
    Assert.assertEquals(TOTAL_LEVELS, snapshot.getTotalMergingLevels());
    Map<Integer, Long> levelToTotalMergersModifiedMap = new HashMap<>(LEVEL_TO_TOTAL_MERGERS_COMMON_MAP);

    // Snapshot should replace the total levels of the ultimate in the Map if it is not set explicitly
    levelToTotalMergersModifiedMap.put(TOTAL_LEVELS - 1, 25L);
    Assert.assertEquals(levelToTotalMergersModifiedMap, snapshot.getLevelToTotalBatches());
    Assert.assertEquals(LEVEL_TO_MERGED_BATCHES_COMMON_MAP, snapshot.getLevelToMergedBatches());
    Assert.assertEquals(false, snapshot.isTriviallyComplete());
  }

  @Test
  public void testSnapshotWhenTrackerIsTriviallyComplete()
  {
    superSorterProgressTracker.setTotalMergingLevels(TOTAL_LEVELS);
    superSorterProgressTracker.setTotalMergersForUltimateLevel(25);
    superSorterProgressTracker.markTriviallyComplete();

    SuperSorterProgressSnapshot snapshot = superSorterProgressTracker.snapshot();
    Assert.assertNotNull(snapshot.getProgressDigest());
    Assert.assertEquals(1.0, snapshot.getProgressDigest(), 0.0);
    Assert.assertEquals(true, snapshot.isTriviallyComplete());
  }
}
