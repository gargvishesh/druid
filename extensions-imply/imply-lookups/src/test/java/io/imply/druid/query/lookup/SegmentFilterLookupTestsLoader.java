/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.query.lookup;

import com.google.common.collect.ImmutableMap;
import com.google.common.io.Files;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.data.input.MapBasedInputRow;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.TimestampSpec;
import org.apache.druid.java.util.common.FileUtils;
import org.apache.druid.math.expr.ExpressionProcessing;
import org.apache.druid.segment.IndexBuilder;
import org.apache.druid.segment.QueryableIndex;
import org.apache.druid.segment.QueryableIndexSegment;
import org.apache.druid.segment.ReferenceCountingSegment;
import org.apache.druid.segment.SegmentLazyLoadFailCallback;
import org.apache.druid.segment.TestIndex;
import org.apache.druid.segment.incremental.IncrementalIndexSchema;
import org.apache.druid.segment.loading.SegmentLoader;
import org.apache.druid.segment.writeout.OnHeapMemorySegmentWriteOutMediumFactory;
import org.apache.druid.timeline.DataSegment;

import java.io.File;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutorService;

@SuppressWarnings("ALL")
class SegmentFilterLookupTestsLoader implements SegmentLoader
{
  private static final QueryableIndex NON_FILTERED_INDEX;
  private static final QueryableIndex FILTERABLE_INDEX;

  static {
    NullHandling.initializeForTests();
    ExpressionProcessing.initializeForTests();

    File tmpDir = FileUtils.createTempDir();
    tmpDir.deleteOnExit();

    final List<String> dimensions = Arrays.asList("colA", "colB", "colC");
    NON_FILTERED_INDEX = IndexBuilder
        .create()
        .tmpDir(tmpDir)
        .segmentWriteOutMediumFactory(OnHeapMemorySegmentWriteOutMediumFactory.instance())
        .schema(new IncrementalIndexSchema.Builder()
                    .withRollup(false)
                    .withTimestampSpec(new TimestampSpec("time", null, null))
                    .withDimensionsSpec(new DimensionsSpec(DimensionsSpec.getDefaultSchemas(dimensions)))
                    .build()
        )
        .rows(
            Arrays.asList(
                new MapBasedInputRow(0, dimensions, ImmutableMap.of("colA", "A", "colB", "bob", "colC", "sally")),
                new MapBasedInputRow(0, dimensions, ImmutableMap.of("colA", "B", "colB", "bill", "colC", "sue")),
                new MapBasedInputRow(0, dimensions, ImmutableMap.of("colA", "C", "colB", "the", "colC", "the")),
                new MapBasedInputRow(0, dimensions, ImmutableMap.of("colA", "D", "colB", "kid", "colC", "seamstress"))
            )
        )
        .buildMMappedIndex();

    FILTERABLE_INDEX = IndexBuilder
        .create()
        .tmpDir(tmpDir)
        .segmentWriteOutMediumFactory(OnHeapMemorySegmentWriteOutMediumFactory.instance())
        .schema(new IncrementalIndexSchema.Builder()
                    .withRollup(false)
                    .withTimestampSpec(new TimestampSpec("time", null, null))
                    .withDimensionsSpec(new DimensionsSpec(DimensionsSpec.getDefaultSchemas(dimensions)))
                    .build()
        )
        .rows(
            Arrays.asList(
                new MapBasedInputRow(0, dimensions, ImmutableMap.of("colA", "A", "colB", "a", "colC", "sally")),
                new MapBasedInputRow(0, dimensions, ImmutableMap.of("colA", "A", "colB", "b", "colC", "sue")),
                new MapBasedInputRow(0, dimensions, ImmutableMap.of("colA", "A", "colB", "c", "colC", "the")),
                new MapBasedInputRow(0, dimensions, ImmutableMap.of("colA", "A", "colB", "d", "colC", "seamstress")),
                new MapBasedInputRow(0, dimensions, ImmutableMap.of("colA", "B", "colB", "a", "colC", "bob")),
                new MapBasedInputRow(0, dimensions, ImmutableMap.of("colA", "B", "colB", "b", "colC", "bill")),
                new MapBasedInputRow(0, dimensions, ImmutableMap.of("colA", "B", "colB", "c", "colC", "the")),
                new MapBasedInputRow(0, dimensions, ImmutableMap.of("colA", "B", "colB", "d", "colC", "kid")),
                new MapBasedInputRow(0, dimensions, ImmutableMap.of("colA", "C", "colB", "a", "colC", "three")),
                new MapBasedInputRow(0, dimensions, ImmutableMap.of("colA", "C", "colB", "b", "colC", "two")),
                new MapBasedInputRow(0, dimensions, ImmutableMap.of("colA", "C", "colB", "c", "colC", "one")),
                new MapBasedInputRow(0, dimensions, ImmutableMap.of("colA", "C", "colB", "d", "colC", "boom")),
                new MapBasedInputRow(0, dimensions, ImmutableMap.of("colA", "C", "colB", "d", "colC", "!!!"))
            )
        )
        .buildMMappedIndex();
  }

  @Override
  public ReferenceCountingSegment getSegment(DataSegment segment, boolean lazy, SegmentLazyLoadFailCallback loadFailed)
  {
    if ("nonFiltered".equals(segment.getDataSource())) {
      return ReferenceCountingSegment.wrapSegment(
          new QueryableIndexSegment(NON_FILTERED_INDEX, segment.getId()),
          segment.getShardSpec()
      );
    } else if ("filterable".equals(segment.getDataSource())) {
      return ReferenceCountingSegment.wrapSegment(
          new QueryableIndexSegment(FILTERABLE_INDEX, segment.getId()),
          segment.getShardSpec()
      );
    } else if ("withCallback".equals(segment.getDataSource())) {
      return ReferenceCountingSegment.wrapSegment(
          new QueryableIndexSegment(TestIndex.getNoRollupMMappedTestIndex(), segment.getId()),
          segment.getShardSpec()
      );
    } else {
      throw new UnsupportedOperationException();
    }
  }

  @Override
  public void cleanup(DataSegment segment)
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public void loadSegmentIntoPageCache(DataSegment segment, ExecutorService exec)
  {

  }
}
