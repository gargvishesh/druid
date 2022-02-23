/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.talaria.frame;

import io.imply.druid.talaria.frame.cluster.ClusterByColumn;
import io.imply.druid.talaria.frame.processor.FrameRowTooLargeException;
import io.imply.druid.talaria.frame.read.Frame;
import io.imply.druid.talaria.frame.write.FrameWriter;
import io.imply.druid.talaria.frame.write.HeapMemoryAllocator;
import org.apache.druid.java.util.common.guava.BaseSequence;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.segment.ColumnSelectorFactory;
import org.apache.druid.segment.Cursor;
import org.apache.druid.segment.StorageAdapter;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

/**
 * Utility for making {@link Frame} instances for testing.
 */
public class TestFrameSequenceBuilder
{
  private final StorageAdapter adapter;

  private MemoryAllocator allocator = null;
  private List<ClusterByColumn> clusterByColumns = new ArrayList<>();
  private int maxRowsPerFrame = Integer.MAX_VALUE;
  private boolean populateRowNumber = false;

  private TestFrameSequenceBuilder(StorageAdapter adapter)
  {
    this.adapter = adapter;
  }

  public static TestFrameSequenceBuilder fromAdapter(final StorageAdapter adapter)
  {
    return new TestFrameSequenceBuilder(adapter);
  }

  public TestFrameSequenceBuilder allocator(final MemoryAllocator allocator)
  {
    this.allocator = allocator;
    return this;
  }

  /**
   * Sorts each frame by the given columns. Does not do any sorting between frames.
   */
  public TestFrameSequenceBuilder sortBy(final List<ClusterByColumn> sortBy)
  {
    this.clusterByColumns = sortBy;
    return this;
  }

  /**
   * Limits each frame to the given size.
   */
  public TestFrameSequenceBuilder maxRowsPerFrame(final int maxRowsPerFrame)
  {
    this.maxRowsPerFrame = maxRowsPerFrame;
    return this;
  }

  public TestFrameSequenceBuilder populateRowNumber()
  {
    this.populateRowNumber = true;
    return this;
  }

  public RowSignature signature()
  {
    if (populateRowNumber) {
      return RowSignature.builder()
                         .addAll(adapter.getRowSignature())
                         .add(FrameTestUtil.ROW_NUMBER_COLUMN, ColumnType.LONG)
                         .build();
    } else {
      return adapter.getRowSignature();
    }
  }

  public Sequence<Frame> frames()
  {
    if (allocator == null) {
      allocator = HeapMemoryAllocator.unlimited();
    }

    final Sequence<Cursor> cursors = FrameTestUtil.makeCursorsForAdapter(adapter, populateRowNumber);
    final RowSignature signature = signature();

    return cursors.flatMap(
        cursor -> new BaseSequence<>(
            new BaseSequence.IteratorMaker<Frame, Iterator<Frame>>()
            {
              @Override
              public Iterator<Frame> make()
              {
                final ColumnSelectorFactory columnSelectorFactory = cursor.getColumnSelectorFactory();

                return new Iterator<Frame>()
                {
                  @Override
                  public boolean hasNext()
                  {
                    return !cursor.isDone();
                  }

                  @Override
                  public Frame next()
                  {
                    if (cursor.isDone()) {
                      throw new NoSuchElementException();
                    }

                    try (final FrameWriter writer = FrameWriter.create(columnSelectorFactory, allocator, signature)) {
                      while (!cursor.isDone()) {
                        if (!writer.addSelection()) {
                          if (writer.getNumRows() == 0) {
                            throw new FrameRowTooLargeException(allocator.capacity());
                          }

                          return makeFrame(writer);
                        }

                        cursor.advance();

                        if (writer.getNumRows() >= maxRowsPerFrame) {
                          return makeFrame(writer);
                        }
                      }

                      return makeFrame(writer);
                    }
                  }

                  private Frame makeFrame(final FrameWriter writer)
                  {
                    writer.sort(clusterByColumns);
                    return Frame.wrap(writer.toByteArray());
                  }
                };
              }

              @Override
              public void cleanup(Iterator<Frame> iterFromMake)
              {
                // Nothing to do.
              }
            }
        )
    );
  }
}
