/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.talaria.indexing;

import io.imply.druid.talaria.frame.channel.FrameWithPartition;
import io.imply.druid.talaria.frame.channel.ReadableFrameChannel;
import io.imply.druid.talaria.frame.channel.WritableFrameChannel;
import io.imply.druid.talaria.frame.cluster.ClusterBy;
import io.imply.druid.talaria.frame.cluster.ClusterByKey;
import io.imply.druid.talaria.frame.cluster.statistics.ClusterByStatisticsCollector;
import io.imply.druid.talaria.frame.processor.FrameProcessor;
import io.imply.druid.talaria.frame.processor.FrameProcessors;
import io.imply.druid.talaria.frame.processor.ReturnOrAwait;
import io.imply.druid.talaria.frame.read.Frame;
import io.imply.druid.talaria.frame.read.FrameReader;
import it.unimi.dsi.fastutil.ints.IntSet;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.UOE;
import org.apache.druid.query.dimension.DefaultDimensionSpec;
import org.apache.druid.segment.BaseObjectColumnValueSelector;
import org.apache.druid.segment.ColumnSelectorFactory;
import org.apache.druid.segment.Cursor;
import org.apache.druid.segment.DimensionSelector;
import org.apache.druid.segment.column.ColumnCapabilities;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.segment.column.ValueType;
import org.apache.druid.segment.data.IndexedInts;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.IntSupplier;
import java.util.function.Supplier;

public class TalariaClusterByStatisticsCollectionProcessor implements FrameProcessor<ClusterByStatisticsCollector>
{
  /**
   * Constant chosen such that a segment full of "standard" rows, row count
   * {@link io.imply.druid.talaria.sql.TalariaQueryMaker#DEFAULT_ROWS_PER_SEGMENT}, and *some* redundancy between
   * rows (therefore: some "reasonable" compression) will not have any columns greater than 2GB in size.
   */
  private static final int STANDARD_VALUE_SIZE = 1000;
  private static final IntSupplier ZERO_ESTIMATOR = () -> 0;

  private final ReadableFrameChannel inputChannel;
  private final WritableFrameChannel outputChannel;
  private final FrameReader frameReader;
  private final ClusterBy clusterBy;

  private ClusterByStatisticsCollector clusterByStatisticsCollector;

  public TalariaClusterByStatisticsCollectionProcessor(
      final ReadableFrameChannel inputChannel,
      final WritableFrameChannel outputChannel,
      final FrameReader frameReader,
      final ClusterBy clusterBy,
      final ClusterByStatisticsCollector clusterByStatisticsCollector
  )
  {
    this.inputChannel = inputChannel;
    this.outputChannel = outputChannel;
    this.frameReader = frameReader;
    this.clusterBy = clusterBy;
    this.clusterByStatisticsCollector = clusterByStatisticsCollector;
  }

  @Override
  public List<ReadableFrameChannel> inputChannels()
  {
    return Collections.singletonList(inputChannel);
  }

  @Override
  public List<WritableFrameChannel> outputChannels()
  {
    return Collections.singletonList(outputChannel);
  }

  @Override
  public ReturnOrAwait<ClusterByStatisticsCollector> runIncrementally(final IntSet readableInputs) throws IOException
  {
    if (readableInputs.isEmpty()) {
      return ReturnOrAwait.awaitAll(1);
    }

    if (inputChannel.isFinished()) {
      return ReturnOrAwait.returnObject(clusterByStatisticsCollector);
    }

    final Frame frame = inputChannel.read().getOrThrow();
    final Cursor cursor = FrameProcessors.makeCursor(frame, frameReader);
    final ColumnSelectorFactory columnSelectorFactory = cursor.getColumnSelectorFactory();
    final Supplier<ClusterByKey> clusterByKeyReader = clusterBy.keyReader(
        columnSelectorFactory,
        frameReader.signature()
    );

    final IntSupplier rowWeightSupplier = makeRowWeightSupplier(frameReader, columnSelectorFactory);

    while (!cursor.isDone()) {
      clusterByStatisticsCollector.add(clusterByKeyReader.get(), rowWeightSupplier.getAsInt());
      cursor.advance();
    }

    // Clears partition info (uses NO_PARTITION), but that's OK, because it isn't needed downstream of this processor.
    outputChannel.write(new FrameWithPartition(frame, FrameWithPartition.NO_PARTITION));
    return ReturnOrAwait.awaitAll(1);
  }

  @Override
  public void cleanup() throws IOException
  {
    FrameProcessors.closeAll(
        inputChannels(),
        outputChannels(),
        () -> clusterByStatisticsCollector = null
    );
  }

  private IntSupplier makeRowWeightSupplier(
      final FrameReader frameReader,
      final ColumnSelectorFactory columnSelectorFactory
  )
  {
    final List<IntSupplier> sizeEstimators = new ArrayList<>();
    final RowSignature signature = frameReader.signature();

    for (int i = 0; i < signature.size(); i++) {
      sizeEstimators.add(makeSizeEstimator(columnSelectorFactory, signature.getColumnName(i)));
    }

    return () -> {
      int maxSize = 0;
      for (final IntSupplier estimator : sizeEstimators) {
        final int size = estimator.getAsInt();
        if (size > maxSize) {
          maxSize = size;
        }
      }

      // Weight is the number of standard value sizes in the biggest column.
      return maxSize / STANDARD_VALUE_SIZE + 1;
    };
  }

  private static IntSupplier makeSizeEstimator(
      final ColumnSelectorFactory columnSelectorFactory,
      final String columnName
  )
  {
    final ColumnCapabilities capabilities = columnSelectorFactory.getColumnCapabilities(columnName);

    if (capabilities == null) {
      return () -> 0;
    }

    switch (capabilities.getType()) {
      case STRING:
        return makeStringSizeEstimator(columnSelectorFactory, columnName);

      case ARRAY:
        if (capabilities.getElementType().getType() == ValueType.STRING) {
          return makeStringArraySizeEstimator(columnSelectorFactory, columnName);
        } else {
          throw new UOE("Arrays of type [%s] are not supported", capabilities.getElementType());
        }

      default:
        // Don't want to waste time checking how big primitives and complex types are. Primitive columns can't be
        // > 2GB at any reasonable number of rows per segment. Complex columns generally support large (> 2GB) columns.
        return ZERO_ESTIMATOR;
    }
  }

  private static IntSupplier makeStringSizeEstimator(
      final ColumnSelectorFactory columnSelectorFactory,
      final String columnName
  )
  {
    final DimensionSelector selector =
        columnSelectorFactory.makeDimensionSelector(DefaultDimensionSpec.of(columnName));

    if (!selector.supportsLookupNameUtf8()) {
      // This class is only used on frames, which support lookupNameUtf8.
      throw new ISE("Expected dimension selector to support lookupNameUtf8");
    }

    return () -> {
      int bytes = 0;

      final IndexedInts row = selector.getRow();
      final int sz = row.size();

      bytes += Integer.BYTES * (sz + 1);

      for (int i = 0; i < sz; i++) {
        final ByteBuffer buf = selector.lookupNameUtf8(i);
        bytes += buf == null ? 0 : buf.remaining();
      }

      return bytes;
    };
  }

  private static IntSupplier makeStringArraySizeEstimator(
      final ColumnSelectorFactory columnSelectorFactory,
      final String columnName
  )
  {
    final BaseObjectColumnValueSelector<List<Object>> selector =
        columnSelectorFactory.makeColumnValueSelector(columnName);

    return () -> {
      final List<Object> arr = selector.getObject();

      if (arr == null) {
        return 0;
      }

      int bytes = Integer.BYTES * (arr.size() + 1);

      for (final Object o : arr) {
        // Not 100% correct; ideally we'd use UTF8 size, not the number of characters. But this estimate
        // doesn't have to be 100% correct to be useful.
        bytes += o == null ? 0 : ((String) o).length();
      }

      return bytes;
    };
  }
}
