/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.talaria.querykit.groupby;

import io.imply.druid.talaria.frame.Frame;
import io.imply.druid.talaria.frame.FrameType;
import io.imply.druid.talaria.frame.MemoryAllocator;
import io.imply.druid.talaria.frame.channel.FrameWithPartition;
import io.imply.druid.talaria.frame.channel.ReadableFrameChannel;
import io.imply.druid.talaria.frame.channel.WritableFrameChannel;
import io.imply.druid.talaria.frame.cluster.ClusterBy;
import io.imply.druid.talaria.frame.processor.FrameProcessor;
import io.imply.druid.talaria.frame.processor.FrameProcessors;
import io.imply.druid.talaria.frame.processor.FrameRowTooLargeException;
import io.imply.druid.talaria.frame.processor.ReturnOrAwait;
import io.imply.druid.talaria.frame.read.FrameReader;
import io.imply.druid.talaria.frame.write.FrameWriter;
import io.imply.druid.talaria.frame.write.FrameWriterFactory;
import io.imply.druid.talaria.frame.write.FrameWriters;
import io.imply.druid.talaria.querykit.QueryWorkerUtils;
import it.unimi.dsi.fastutil.ints.IntSet;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.PostAggregator;
import org.apache.druid.query.groupby.GroupByQuery;
import org.apache.druid.query.groupby.ResultRow;
import org.apache.druid.query.groupby.epinephelinae.RowBasedGrouperHelper;
import org.apache.druid.query.groupby.having.AlwaysHavingSpec;
import org.apache.druid.query.groupby.having.DimFilterHavingSpec;
import org.apache.druid.query.groupby.having.HavingSpec;
import org.apache.druid.query.groupby.strategy.GroupByStrategySelector;
import org.apache.druid.segment.ColumnSelectorFactory;
import org.apache.druid.segment.ColumnValueSelector;
import org.apache.druid.segment.Cursor;
import org.apache.druid.segment.VirtualColumn;
import org.apache.druid.segment.VirtualColumns;
import org.apache.druid.segment.column.RowSignature;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.function.BinaryOperator;
import java.util.function.Consumer;
import java.util.function.Supplier;

public class GroupByPostShuffleFrameProcessor implements FrameProcessor<Long>
{
  private final GroupByQuery query;
  private final ReadableFrameChannel inputChannel;
  private final WritableFrameChannel outputChannel;
  private final MemoryAllocator allocator;
  private final FrameReader frameReader;
  private final RowSignature resultSignature;
  private final ClusterBy clusterBy;
  private final ColumnSelectorFactory columnSelectorFactoryForFrameWriter;
  private final Comparator<ResultRow> compareFn;
  private final BinaryOperator<ResultRow> mergeFn;
  private final Consumer<ResultRow> finalizeFn;

  @Nullable
  private final HavingSpec havingSpec;

  private Cursor frameCursor = null;
  private Supplier<ResultRow> rowSupplierFromFrameCursor;
  private ResultRow outputRow = null;
  private FrameWriter frameWriter = null;

  public GroupByPostShuffleFrameProcessor(
      final GroupByQuery query,
      final GroupByStrategySelector strategySelector,
      final ReadableFrameChannel inputChannel,
      final WritableFrameChannel outputChannel,
      final FrameReader frameReader,
      final RowSignature resultSignature,
      final ClusterBy clusterBy,
      final MemoryAllocator allocator
  )
  {
    this.query = query;
    this.inputChannel = inputChannel;
    this.outputChannel = outputChannel;
    this.frameReader = frameReader;
    this.resultSignature = resultSignature;
    this.clusterBy = clusterBy;
    this.allocator = allocator;
    this.compareFn = strategySelector.strategize(query).createResultComparator(query);
    this.mergeFn = strategySelector.strategize(query).createMergeFn(query);
    this.finalizeFn = makeFinalizeFn(query);
    this.havingSpec = cloneHavingSpec(query);
    this.columnSelectorFactoryForFrameWriter =
        makeVirtualColumnsForFrameWriter(query).wrap(
            RowBasedGrouperHelper.createResultRowBasedColumnSelectorFactory(
                query,
                () -> outputRow,
                RowSignature.Finalization.YES
            )
        );
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
  public ReturnOrAwait<Long> runIncrementally(final IntSet readableInputs) throws IOException
  {
    if (frameCursor == null || frameCursor.isDone()) {
      // Keep reading through the input channel.
      if (readableInputs.isEmpty()) {
        return ReturnOrAwait.awaitAll(1);
      } else if (inputChannel.isFinished()) {
        if (outputRow != null && writeOutputRow()) {
          return ReturnOrAwait.runAgain();
        }

        writeCurrentFrameIfNeeded();
        return ReturnOrAwait.returnObject(0L);
      } else {
        final Frame frame = inputChannel.read().getOrThrow();
        frameCursor = FrameProcessors.makeCursor(frame, frameReader);
        final ColumnSelectorFactory frameColumnSelectorFactory = frameCursor.getColumnSelectorFactory();

        //noinspection unchecked
        final Supplier<Object>[] fieldSuppliers = new Supplier[query.getResultRowSizeWithoutPostAggregators()];
        for (int i = 0; i < fieldSuppliers.length; i++) {
          final ColumnValueSelector<?> selector =
              frameColumnSelectorFactory.makeColumnValueSelector(frameReader.signature().getColumnName(i));
          fieldSuppliers[i] = selector::getObject;
        }

        final int fullRowSize = query.getResultRowSignature().size();
        rowSupplierFromFrameCursor = () -> {
          final ResultRow row = ResultRow.create(fullRowSize);
          for (int i = 0; i < fieldSuppliers.length; i++) {
            row.set(i, fieldSuppliers[i].get());
          }

          for (int i = fieldSuppliers.length; i < fullRowSize; i++) {
            // Post-aggregators.
            row.set(i, null);
          }

          return row;
        };
      }
    }

    setUpFrameWriterIfNeeded();

    while (!frameCursor.isDone()) {
      final ResultRow currentRow = rowSupplierFromFrameCursor.get();

      if (outputRow == null) {
        outputRow = currentRow.copy();
      } else if (compareFn.compare(outputRow, currentRow) == 0) {
        outputRow = mergeFn.apply(outputRow, currentRow);
      } else {
        if (writeOutputRow()) {
          return ReturnOrAwait.runAgain();
        }

        outputRow = currentRow.copy();
      }

      frameCursor.advance();
    }

    return ReturnOrAwait.runAgain();
  }

  @Override
  public void cleanup() throws IOException
  {
    FrameProcessors.closeAll(inputChannels(), outputChannels(), frameWriter);
  }

  /**
   * Writes the current {@link #outputRow} to a pending frame, if it matches the query's havingSpec. Either way,
   * the {@link #outputRow} is cleared.
   *
   * If needed, writes the previously pending frame to the output channel.
   *
   * @return whether the previously pending frame was flushed
   */
  private boolean writeOutputRow() throws IOException
  {
    // TODO(gianm): hack: the mergeFn can shrink the row too much; re-expand it.
    final int resultRowSize = query.getResultRowSignature().size();

    if (outputRow.length() < resultRowSize) {
      final Object[] newArray = new Object[resultRowSize];
      System.arraycopy(outputRow.getArray(), 0, newArray, 0, outputRow.length());
      outputRow = ResultRow.of(newArray);
    }

    // Apply post-aggregators.
    // TODO(gianm): copy-pasted code from GroupByStrategyV2
    final Map<String, Object> outputRowAsMap = outputRow.toMap(query);

    for (int i = 0; i < query.getPostAggregatorSpecs().size(); i++) {
      final PostAggregator postAggregator = query.getPostAggregatorSpecs().get(i);
      final Object value = postAggregator.compute(outputRowAsMap);
      outputRow.set(query.getResultRowPostAggregatorStart() + i, value);
      outputRowAsMap.put(postAggregator.getName(), value);
    }

    // Finalize aggregators.
    finalizeFn.accept(outputRow);

    if (havingSpec != null && !havingSpec.eval(outputRow)) {
      // Didn't match HAVING.
      outputRow = null;
      return false;
    } else if (frameWriter.addSelection()) {
      outputRow = null;
      return false;
    } else if (frameWriter.getNumRows() > 0) {
      writeCurrentFrameIfNeeded();
      setUpFrameWriterIfNeeded();

      if (frameWriter.addSelection()) {
        outputRow = null;
        return true;
      } else {
        throw new FrameRowTooLargeException(allocator.capacity());
      }
    } else {
      throw new FrameRowTooLargeException(allocator.capacity());
    }
  }

  private void writeCurrentFrameIfNeeded() throws IOException
  {
    if (frameWriter != null && frameWriter.getNumRows() > 0) {
      final Frame frame = Frame.wrap(frameWriter.toByteArray());
      outputChannel.write(new FrameWithPartition(frame, FrameWithPartition.NO_PARTITION));
      frameWriter.close();
      frameWriter = null;
    }
  }

  private void setUpFrameWriterIfNeeded()
  {
    if (frameWriter == null) {
      final FrameWriterFactory frameWriterFactory =
          FrameWriters.makeFrameWriterFactory(FrameType.ROW_BASED, allocator, resultSignature, clusterBy.getColumns());
      frameWriter = frameWriterFactory.newFrameWriter(columnSelectorFactoryForFrameWriter);
    }
  }

  private static Consumer<ResultRow> makeFinalizeFn(final GroupByQuery query)
  {
    if (GroupByQueryKit.isFinalize(query)) {
      final int startIndex = query.getResultRowAggregatorStart();
      final List<AggregatorFactory> aggregators = query.getAggregatorSpecs();

      return row -> {
        for (int i = 0; i < aggregators.size(); i++) {
          row.set(startIndex + i, aggregators.get(i).finalizeComputation(row.get(startIndex + i)));
        }
      };
    } else {
      return row -> {};
    }
  }

  @Nullable
  private static HavingSpec cloneHavingSpec(final GroupByQuery query)
  {
    if (query.getHavingSpec() == null || query.getHavingSpec() instanceof AlwaysHavingSpec) {
      return null;
    } else if (query.getHavingSpec() instanceof DimFilterHavingSpec) {
      final DimFilterHavingSpec dimFilterHavingSpec = (DimFilterHavingSpec) query.getHavingSpec();
      final DimFilterHavingSpec clonedHavingSpec = new DimFilterHavingSpec(
          dimFilterHavingSpec.getDimFilter(),
          dimFilterHavingSpec.isFinalize()
      );
      clonedHavingSpec.setQuery(query);
      return clonedHavingSpec;
    } else {
      // TODO(gianm): Only works with AlwaysHavingSpec and DimFilterHavingSpec
      throw new UnsupportedOperationException("Must use 'filter' or 'always' havingSpec");
    }
  }

  /**
   * Create virtual columns containing "bonus" fields that should be attached to the {@link FrameWriter} for
   * this processor. Kept in sync with the signature generated by {@link GroupByQueryKit}.
   */
  private static VirtualColumns makeVirtualColumnsForFrameWriter(final GroupByQuery query)
  {
    final VirtualColumn segmentGranularityVirtualColumn = QueryWorkerUtils.makeSegmentGranularityVirtualColumn(query);

    if (segmentGranularityVirtualColumn == null) {
      return VirtualColumns.EMPTY;
    } else {
      return VirtualColumns.create(Collections.singletonList(segmentGranularityVirtualColumn));
    }
  }
}
