/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.segment.join;

import com.google.common.collect.ImmutableSet;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.segment.column.ValueType;
import org.apache.druid.segment.join.table.IndexedTable;
import org.apache.druid.timeline.SegmentId;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.Closeable;
import java.io.IOException;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutorService;

public class CloseTableFirstReferenceCountingIndexedTableTest
{
  private static final int EXPECTED_NUM_ROWS = 1000;
  private static final String EXPECTED_VERSION = "v0";
  private static final Set<String> EXPECTED_KEY_COLUMNS = ImmutableSet.of("a", "b", "c");
  private static final RowSignature EXPECTED_SIGNATURE = RowSignature.builder()
                                                                     .add("a", ValueType.STRING)
                                                                     .add("b", ValueType.LONG)
                                                                     .add("c", ValueType.DOUBLE)
                                                                     .build();

  private boolean baseTableIsClosed = false;
  private boolean refsAcquired = true;
  private ExecutorService exec;

  @Before
  public void setup()
  {
    exec = Execs.multiThreaded(2, "ref-counting-test-%s");
  }

  @Test
  public void testWrapsTable()
  {
    CloseTableFirstReferenceCountingIndexedTable table = new CloseTableFirstReferenceCountingIndexedTable(
        new ExpectedIndexedTable(),
        SegmentId.of("datasource", Intervals.ETERNITY, EXPECTED_VERSION, null)
    );
    Assert.assertEquals(EXPECTED_NUM_ROWS, table.numRows());
    Assert.assertEquals(EXPECTED_KEY_COLUMNS, table.keyColumns());
    Assert.assertEquals(EXPECTED_VERSION, table.version());
    Assert.assertEquals(EXPECTED_KEY_COLUMNS, table.keyColumns());
    Assert.assertEquals(Intervals.ETERNITY, table.getSegmentId().getInterval());
  }

  @Test
  public void testCloseBlocksUntilAllClosed()
  {
    CloseTableFirstReferenceCountingIndexedTable table = new CloseTableFirstReferenceCountingIndexedTable(
        new ExpectedIndexedTable(),
        SegmentId.of("datasource", Intervals.ETERNITY, EXPECTED_VERSION, null)
    );

    Optional<Closeable> refCloser = table.acquireReferences();
    Optional<Closeable> refCloser2 = table.acquireReferences();
    Assert.assertTrue(refsAcquired);
    Assert.assertTrue(refCloser.isPresent());

    exec.submit(() -> {
      try {
        Thread.sleep(1000);
        refCloser.get().close();
      }
      catch (InterruptedException | IOException e) {
        throw new RuntimeException("failed", e);
      }
    });
    exec.submit(() -> {
      try {
        Thread.sleep(2000);
        refCloser2.get().close();
      }
      catch (InterruptedException | IOException e) {
        throw new RuntimeException("failed", e);
      }
    });

    Assert.assertFalse(baseTableIsClosed);

    table.close();

    Assert.assertTrue(baseTableIsClosed);
  }

  private class ExpectedIndexedTable implements IndexedTable
  {
    @Override
    public String version()
    {
      return EXPECTED_VERSION;
    }

    @Override
    public Set<String> keyColumns()
    {
      return EXPECTED_KEY_COLUMNS;
    }

    @Override
    public RowSignature rowSignature()
    {
      return EXPECTED_SIGNATURE;
    }

    @Override
    public int numRows()
    {
      return EXPECTED_NUM_ROWS;
    }

    @Override
    public Index columnIndex(int i)
    {
      return null;
    }

    @Override
    public Reader columnReader(int i)
    {
      return null;
    }

    @Override
    public void close()
    {
      baseTableIsClosed = true;
    }

    @Override
    public Optional<Closeable> acquireReferences()
    {
      refsAcquired = true;
      return Optional.of(() -> {});
    }
  }
}
