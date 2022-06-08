/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.talaria.frame.write;

import com.google.common.collect.Iterables;
import io.imply.druid.talaria.frame.ArenaMemoryAllocator;
import io.imply.druid.talaria.frame.Frame;
import io.imply.druid.talaria.frame.FrameType;
import io.imply.druid.talaria.frame.HeapMemoryAllocator;
import io.imply.druid.talaria.frame.MemoryAllocator;
import io.imply.druid.talaria.frame.channel.BlockingQueueFrameChannel;
import io.imply.druid.talaria.frame.channel.FrameWithPartition;
import io.imply.druid.talaria.frame.cluster.ClusterBy;
import io.imply.druid.talaria.frame.cluster.ClusterByColumn;
import io.imply.druid.talaria.frame.cluster.ClusterByKey;
import io.imply.druid.talaria.frame.cluster.ClusterByTestUtils;
import io.imply.druid.talaria.frame.read.FrameReader;
import io.imply.druid.talaria.frame.segment.FrameSegment;
import io.imply.druid.talaria.frame.testutil.FrameTestUtil;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.java.util.common.RE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.guava.Sequences;
import org.apache.druid.query.aggregation.hyperloglog.HyperUniquesSerde;
import org.apache.druid.segment.RowBasedSegment;
import org.apache.druid.segment.Segment;
import org.apache.druid.segment.VirtualColumns;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.segment.column.ValueType;
import org.apache.druid.segment.serde.ComplexMetrics;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.apache.druid.timeline.SegmentId;
import org.hamcrest.CoreMatchers;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Tests that exercise {@link FrameWriter} implementations.
 */
@RunWith(Parameterized.class)
public class FrameWriterTest extends InitializedNullHandlingTest
{
  private static final int DEFAULT_ALLOCATOR_CAPACITY = 1_000_000;

  @Nullable
  private final FrameType inputFrameType;
  private final FrameType outputFrameType;
  private final FrameWriterTestData.Sortedness sortedness;

  private MemoryAllocator allocator;

  public FrameWriterTest(
      @Nullable final FrameType inputFrameType,
      final FrameType outputFrameType,
      final FrameWriterTestData.Sortedness sortedness
  )
  {
    this.inputFrameType = inputFrameType;
    this.outputFrameType = outputFrameType;
    this.sortedness = sortedness;
    this.allocator = ArenaMemoryAllocator.createOnHeap(DEFAULT_ALLOCATOR_CAPACITY);
  }

  @Parameterized.Parameters(name = "inputFrameType = {0}, outputFrameType = {1}, sorted = {2}")
  public static Iterable<Object[]> constructorFeeder()
  {
    final List<Object[]> constructors = new ArrayList<>();

    final Iterable<FrameType> inputFrameTypes = Iterables.concat(
        Collections.singletonList(null), // null means input is not a frame
        Arrays.asList(FrameType.values())
    );

    for (final FrameType inputFrameType : inputFrameTypes) {
      for (final FrameType outputFrameType : FrameType.values()) {
        for (final FrameWriterTestData.Sortedness sortedness : FrameWriterTestData.Sortedness.values()) {
          // Only do sortedness tests for row-based frames. (Columnar frames cannot be sorted.)
          if (sortedness == FrameWriterTestData.Sortedness.UNSORTED || outputFrameType == FrameType.ROW_BASED) {
            constructors.add(new Object[]{inputFrameType, outputFrameType, sortedness});
          }
        }
      }
    }

    return constructors;
  }

  @BeforeClass
  public static void setUpClass()
  {
    ComplexMetrics.registerSerde("hyperUnique", new HyperUniquesSerde());
  }

  @Test
  public void test_string() throws Exception
  {
    testWithDataset(FrameWriterTestData.TEST_STRINGS_SINGLE_VALUE);
  }

  @Test
  public void test_multiValueString() throws Exception
  {
    testWithDataset(FrameWriterTestData.TEST_STRINGS_MULTI_VALUE);
  }

  @Test
  public void test_arrayString() throws Exception
  {
    testWithDataset(FrameWriterTestData.TEST_ARRAYS_STRING);
  }

  @Test
  public void test_long() throws Exception
  {
    testWithDataset(FrameWriterTestData.TEST_LONGS);
  }

  @Test
  public void test_float() throws Exception
  {
    testWithDataset(FrameWriterTestData.TEST_FLOATS);
  }

  @Test
  public void test_double() throws Exception
  {
    testWithDataset(FrameWriterTestData.TEST_DOUBLES);
  }

  @Test
  public void test_complex() throws Exception
  {
    // Complex types can't be sorted, so skip the sortedness tests.
    Assume.assumeThat(sortedness, CoreMatchers.is(FrameWriterTestData.Sortedness.UNSORTED));
    testWithDataset(FrameWriterTestData.TEST_COMPLEX);
  }

  @Test
  public void test_typePairs()
  {
    // Test all possible arrangements of two different types.
    for (final FrameWriterTestData.Dataset<?> dataset1 : FrameWriterTestData.DATASETS) {
      for (final FrameWriterTestData.Dataset<?> dataset2 : FrameWriterTestData.DATASETS) {
        final RowSignature signature = makeSignature(Arrays.asList(dataset1, dataset2));
        final Sequence<List<Object>> rowSequence = unsortAndMakeRows(Arrays.asList(dataset1, dataset2));

        // Sort by all columns up to the first COMPLEX one. (Can't sort by COMPLEX.)
        final List<String> sortColumns = new ArrayList<>();
        if (!dataset1.getType().is(ValueType.COMPLEX)) {
          sortColumns.add(signature.getColumnName(0));

          if (!dataset2.getType().is(ValueType.COMPLEX)) {
            sortColumns.add(signature.getColumnName(1));
          }
        }

        try {
          final Pair<Frame, Integer> writeResult = writeFrame(rowSequence, signature, sortColumns);
          Assert.assertEquals(rowSequence.toList().size(), (int) writeResult.rhs);
          verifyFrame(sortIfNeeded(rowSequence, signature, sortColumns), writeResult.lhs, signature);
        }
        catch (AssertionError e) {
          throw new AssertionError(
              StringUtils.format(
                  "Assert failed in test (%s, %s)",
                  dataset1.getType(),
                  dataset2.getType()
              ),
              e
          );
        }
        catch (Throwable e) {
          throw new RE(e, "Exception in test (%s, %s)", dataset1.getType(), dataset2.getType());
        }
      }
    }
  }

  @Test
  public void test_insufficientWriteCapacity() throws Exception
  {
    // Test every possible capacity, up to the amount required to write all items from every list.
    final RowSignature signature = makeSignature(FrameWriterTestData.DATASETS);
    final Sequence<List<Object>> rowSequence = unsortAndMakeRows(FrameWriterTestData.DATASETS);
    final int totalRows = rowSequence.toList().size();

    // Sort by all columns up to the first COMPLEX one. (Can't sort by COMPLEX.)
    final List<String> sortColumns = new ArrayList<>();
    for (int i = 0; i < signature.size(); i++) {
      if (signature.getColumnType(i).get().is(ValueType.COMPLEX)) {
        break;
      } else {
        sortColumns.add(signature.getColumnName(i));
      }
    }

    final ByteBuffer allocatorMemory = ByteBuffer.wrap(new byte[DEFAULT_ALLOCATOR_CAPACITY]);

    boolean didWritePartial = false;
    int allocatorSize = 0;

    Pair<Frame, Integer> writeResult;

    do {
      allocatorMemory.limit(allocatorSize);
      allocatorMemory.position(0);
      allocator = ArenaMemoryAllocator.create(allocatorMemory);

      try {
        writeResult = writeFrame(rowSequence, signature, sortColumns);

        final int rowsWritten = writeResult.rhs;

        if (writeResult.rhs > 0 && writeResult.rhs < totalRows) {
          didWritePartial = true;

          verifyFrame(
              sortIfNeeded(rowSequence.limit(rowsWritten), signature, sortColumns),
              writeResult.lhs,
              signature
          );
        }
      }
      catch (Throwable e) {
        throw new RE(e, "Exception while writing with allocatorSize = %s", allocatorSize);
      }

      allocatorSize++;
    } while (writeResult.rhs != totalRows);

    verifyFrame(sortIfNeeded(rowSequence, signature, sortColumns), writeResult.lhs, signature);

    // We expect that at some point in this test, a partial frame would have been written. If not: that's strange
    // and may mean the test isn't testing the right thing.
    Assert.assertTrue("did write a partial frame", didWritePartial);
  }

  /**
   * Verifies that a frame has a certain set of expected rows. The set of expected rows will be reordered according
   * to the current {@link #sortedness} parameter.
   */
  private void verifyFrame(
      final Sequence<List<Object>> expectedRows,
      final Frame frame,
      final RowSignature signature
  ) throws Exception
  {
    final BlockingQueueFrameChannel singleFrameChannel = BlockingQueueFrameChannel.minimal();
    singleFrameChannel.write(new FrameWithPartition(frame, FrameWithPartition.NO_PARTITION));
    singleFrameChannel.doneWriting();

    FrameTestUtil.assertRowsEqual(
        expectedRows,
        FrameTestUtil.readRowsFromFrameChannel(singleFrameChannel, FrameReader.create(signature))
    );
  }

  /**
   * Sort according to the current {@link #sortedness} parameter.
   */
  private Sequence<List<Object>> sortIfNeeded(
      final Sequence<List<Object>> rows,
      final RowSignature signature,
      final List<String> sortColumns
  )
  {
    final List<ClusterByColumn> clusterByColumns = computeClusterByColumns(sortColumns);

    if (clusterByColumns.isEmpty()) {
      return rows;
    }

    final RowSignature keySignature = ClusterByTestUtils.createKeySignature(clusterByColumns, signature);
    final Comparator<ClusterByKey> keyComparator = new ClusterBy(clusterByColumns, 0).keyComparator();

    return Sequences.sort(
        rows,
        Comparator.comparing(
            row -> ClusterByTestUtils.createKey(keySignature, row.toArray()),
            keyComparator
        )
    );
  }

  /**
   * Writes as many rows to a single frame as possible. Returns the number of rows written.
   */
  private Pair<Frame, Integer> writeFrame(
      final Sequence<List<Object>> rows,
      final RowSignature signature,
      final List<String> sortColumns
  )
  {
    return writeFrame(
        inputFrameType,
        outputFrameType,
        allocator,
        rows,
        signature,
        computeClusterByColumns(sortColumns)
    );
  }

  /**
   * Converts the provided column names into {@link ClusterByColumn} according to the current {@link #sortedness}
   * parameter.
   */
  private List<ClusterByColumn> computeClusterByColumns(final List<String> sortColumns)
  {
    if (sortedness == FrameWriterTestData.Sortedness.UNSORTED) {
      return Collections.emptyList();
    } else {
      return sortColumns.stream()
                        .map(
                            columnName ->
                                new ClusterByColumn(columnName, sortedness == FrameWriterTestData.Sortedness.DESCENDING)
                        )
                        .collect(Collectors.toList());
    }
  }

  private <T> void testWithDataset(final FrameWriterTestData.Dataset<T> dataset) throws Exception
  {
    final List<T> data = dataset.getData(FrameWriterTestData.Sortedness.UNSORTED);
    final RowSignature signature = RowSignature.builder().add("x", dataset.getType()).build();
    final Sequence<List<Object>> rowSequence = rows(data);
    final Pair<Frame, Integer> writeResult = writeFrame(rowSequence, signature, signature.getColumnNames());

    Assert.assertEquals(data.size(), (int) writeResult.rhs);
    verifyFrame(rows(dataset.getData(sortedness)), writeResult.lhs, signature);
  }

  /**
   * Writes as many rows to a single frame as possible. Returns the number of rows written.
   */
  private static Pair<Frame, Integer> writeFrame(
      @Nullable final FrameType inputFrameType,
      final FrameType outputFrameType,
      final MemoryAllocator allocator,
      final Sequence<List<Object>> rows,
      final RowSignature signature,
      final List<ClusterByColumn> sortColumns
  )
  {
    final Segment inputSegment;

    if (inputFrameType == null) {
      // inputFrameType null means input is not a frame
      inputSegment = new RowBasedSegment<>(
          SegmentId.dummy("dummy"),
          rows,
          columnName -> {
            final int columnNumber = signature.indexOf(columnName);
            return row -> columnNumber >= 0 ? row.get(columnNumber) : null;
          },
          signature
      );
    } else {
      final Frame inputFrame = writeFrame(
          null,
          inputFrameType,
          HeapMemoryAllocator.unlimited(),
          rows,
          signature,
          Collections.emptyList()
      ).lhs;

      inputSegment = new FrameSegment(inputFrame, FrameReader.create(signature), SegmentId.dummy("xxx"));
    }

    return inputSegment.asStorageAdapter()
                       .makeCursors(null, Intervals.ETERNITY, VirtualColumns.EMPTY, Granularities.ALL, false, null)
                       .accumulate(
                           null,
                           (retVal, cursor) -> {
                             int numRows = 0;
                             final FrameWriter frameWriter =
                                 FrameWriters.makeFrameWriterFactory(outputFrameType, allocator, signature, sortColumns)
                                             .newFrameWriter(cursor.getColumnSelectorFactory());

                             while (!cursor.isDone() && frameWriter.addSelection()) {
                               numRows++;
                               cursor.advance();
                             }

                             return Pair.of(Frame.wrap(frameWriter.toByteArray()), numRows);
                           }
                       );
  }

  /**
   * Returns a filler value for "type" if "o" is null. Used to pad value lists to the correct length.
   */
  @Nullable
  private static Object fillerValueForType(final ValueType type)
  {
    switch (type) {
      case LONG:
        return NullHandling.defaultLongValue();
      case FLOAT:
        return NullHandling.defaultFloatValue();
      case DOUBLE:
        return NullHandling.defaultDoubleValue();
      case ARRAY:
        return Collections.emptyList();
      default:
        return null;
    }
  }

  /**
   * Create a row signature out of columnar lists of values.
   */
  private static RowSignature makeSignature(final List<FrameWriterTestData.Dataset<?>> datasets)
  {
    final RowSignature.Builder signatureBuilder = RowSignature.builder();

    for (int i = 0; i < datasets.size(); i++) {
      final FrameWriterTestData.Dataset<?> dataset = datasets.get(i);
      signatureBuilder.add(StringUtils.format("col%03d", i), dataset.getType());
    }

    return signatureBuilder.build();
  }

  /**
   * Create rows out of shuffled (unsorted) datasets.
   */
  private static Sequence<List<Object>> unsortAndMakeRows(final List<FrameWriterTestData.Dataset<?>> datasets)
  {
    final List<List<Object>> retVal = new ArrayList<>();

    final int rowSize = datasets.size();
    final List<Iterator<?>> iterators =
        datasets.stream()
                .map(dataset -> dataset.getData(FrameWriterTestData.Sortedness.UNSORTED).iterator())
                .collect(Collectors.toList());

    while (iterators.stream().anyMatch(Iterator::hasNext)) {
      final List<Object> row = new ArrayList<>(rowSize);

      for (int i = 0; i < rowSize; i++) {
        if (iterators.get(i).hasNext()) {
          row.add(iterators.get(i).next());
        } else {
          row.add(fillerValueForType(datasets.get(i).getType().getType()));
        }
      }

      retVal.add(row);
    }

    return Sequences.simple(retVal);
  }

  /**
   * Create a sequence of rows from a list of values. Each value appears in its own row.
   */
  private static Sequence<List<Object>> rows(final List<?> vals)
  {
    final List<List<Object>> retVal = new ArrayList<>();

    for (final Object val : vals) {
      retVal.add(Collections.singletonList(val));
    }

    return Sequences.simple(retVal);
  }
}
