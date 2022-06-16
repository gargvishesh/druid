/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.query.samplinggroupby.engine.twopass;

import com.google.common.collect.ImmutableList;
import io.imply.druid.query.samplinggroupby.SamplingGroupByQuery;
import io.imply.druid.query.samplinggroupby.hashing.HashVectorSupplier;
import io.imply.druid.query.samplinggroupby.hashing.HashingVectorColumnProcessorFactory;
import io.imply.druid.query.samplinggroupby.metrics.SamplingGroupByQueryMetrics;
import io.imply.druid.query.samplinggroupby.virtualcolumns.DimensionHashVirtualColumn;
import io.imply.druid.query.samplinggroupby.virtualcolumns.OffsetFetchingVirtualColumn;
import org.apache.datasketches.Util;
import org.apache.datasketches.hash.MurmurHash3;
import org.apache.datasketches.memory.WritableMemory;
import org.apache.datasketches.theta.RawHashHeapQuickSelectSketch;
import org.apache.druid.collections.ResourceHolder;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.guava.ConcatSequence;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.guava.Sequences;
import org.apache.druid.math.expr.ExprEval;
import org.apache.druid.query.QueryContexts;
import org.apache.druid.query.dimension.DefaultDimensionSpec;
import org.apache.druid.query.filter.AndDimFilter;
import org.apache.druid.query.filter.BoundDimFilter;
import org.apache.druid.query.filter.Filter;
import org.apache.druid.query.groupby.GroupByQuery;
import org.apache.druid.query.groupby.GroupByQueryConfig;
import org.apache.druid.query.groupby.ResultRow;
import org.apache.druid.query.groupby.epinephelinae.vector.VectorGroupByEngine;
import org.apache.druid.query.ordering.StringComparators;
import org.apache.druid.query.vector.VectorCursorGranularizer;
import org.apache.druid.segment.ColumnProcessors;
import org.apache.druid.segment.StorageAdapter;
import org.apache.druid.segment.VirtualColumn;
import org.apache.druid.segment.VirtualColumns;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.vector.SingleValueDimensionVectorSelector;
import org.apache.druid.segment.vector.VectorColumnSelectorFactory;
import org.apache.druid.segment.vector.VectorCursor;
import org.apache.druid.segment.virtual.ExpressionVirtualColumn;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class SamplingGroupByTwoPassQueryEngine
{
  public static final String OFFSET_FETCHER_VIRTUAL_COLUMN_NAME = "_offset";

  public static Sequence<ResultRow> process(
      SamplingGroupByQuery query,
      StorageAdapter storageAdapter,
      ResourceHolder<ByteBuffer> bufferHolder,
      @Nullable Filter filter,
      Interval interval,
      @Nullable SamplingGroupByQueryMetrics samplingGroupByQueryMetrics
  )
  {
    try {
      List<VirtualColumn> virtualColumns = new ArrayList<>(Arrays.asList(query.getVirtualColumns()
                                                                              .getVirtualColumns()));
      OffsetFetchingVirtualColumn offsetFetchingVirtualColumn =
          new OffsetFetchingVirtualColumn(OFFSET_FETCHER_VIRTUAL_COLUMN_NAME);
      virtualColumns.add(offsetFetchingVirtualColumn);
      VectorCursor cursor = storageAdapter.makeVectorCursor(
          filter,
          interval,
          VirtualColumns.create(virtualColumns),
          query.isDescending(),
          QueryContexts.getVectorSize(query),
          samplingGroupByQueryMetrics
      );

      if (cursor == null) {
        // Return empty iterator.
        return Sequences.empty();
      }

      VectorColumnSelectorFactory columnSelectorFactory = cursor.getColumnSelectorFactory();

      // assign memory for 1st pass
      // TODO : some optimization scope for memory allocated in granularity - but maybe not worth it
      // currently, it always assumes total segment rows in every grain
      int hashBytes = getMaxHashBytes(storageAdapter);
      ByteBuffer hashBuffer = bufferHolder.get().duplicate();
      if (hashBuffer.remaining() < hashBytes) {
        throw new ISE("Insufficient memory");
      }
      hashBuffer.limit(hashBuffer.position() + hashBytes);
      WritableMemory memory = WritableMemory.writableWrap(hashBuffer.slice(), ByteOrder.nativeOrder());
      int maxVectorSize = columnSelectorFactory.getMaxVectorSize();
      VectorCursorGranularizer vectorCursorGranularizer = VectorCursorGranularizer.create(
          storageAdapter,
          cursor,
          query.getGranularity(),
          interval
      );
      if (vectorCursorGranularizer == null) {
        return Sequences.empty();
      }
      Iterator<Interval> timeBuckets = vectorCursorGranularizer.getBucketIterable().iterator();
      List<HashVectorSupplier> hashSelectors = query
          .getDimensions()
          .stream()
          .map(dimensionSpec -> ColumnProcessors.makeVectorProcessor(
              dimensionSpec,
              HashingVectorColumnProcessorFactory.instance(),
              columnSelectorFactory
          ))
          .collect(Collectors.toList());
      ImmutableList.Builder<Sequence<ResultRow>> grainResultSequences = ImmutableList.builder();
      int[] offsets = new int[maxVectorSize];
      int offsetsLeft = 0, offsetIdx = 0;
      try {
        while (!cursor.isDone()) {
          Interval currentInterval = timeBuckets.next();
          long[][] rowHashes = new long[maxVectorSize][hashSelectors.size() + 1];
          long[] finalHashes = new long[maxVectorSize];
          RawHashHeapQuickSelectSketch rawHashHeapQuickSelectSketch =
              RawHashHeapQuickSelectSketch.create(Math.max(query.getMaxGroups(), 16), Util.DEFAULT_UPDATE_SEED);
          long tsHash = query.isIntermediateResultRowWithTimestamp() ?
                        MurmurHash3.hash(currentInterval.getStartMillis(), Util.DEFAULT_UPDATE_SEED)[0] >>> 1 : 0;
          memory.fill((byte) -1);
          SingleValueDimensionVectorSelector offsetFetcher = columnSelectorFactory.makeSingleValueDimensionSelector(
              DefaultDimensionSpec.of(OFFSET_FETCHER_VIRTUAL_COLUMN_NAME)
          );
          while (true) {
            vectorCursorGranularizer.setCurrentOffsets(currentInterval);
            int startOffset = vectorCursorGranularizer.getStartOffset();
            int endOffset = vectorCursorGranularizer.getEndOffset();

            if (endOffset > startOffset) {
              for (int selectorId = 0; selectorId < hashSelectors.size(); selectorId++) {
                long[] columnHashes = hashSelectors.get(selectorId).getHashes(startOffset, endOffset);
                for (int vRowId = startOffset; vRowId < endOffset; vRowId++) {
                  rowHashes[vRowId - startOffset][selectorId] = columnHashes[vRowId - startOffset];
                }
              }
              if (query.isIntermediateResultRowWithTimestamp()) {
                for (int vRowId = startOffset; vRowId < endOffset; vRowId++) {
                  rowHashes[vRowId - startOffset][hashSelectors.size()] = tsHash;
                }
              }
              for (int vRowId = startOffset; vRowId < endOffset; vRowId++) {
                finalHashes[vRowId - startOffset] = MurmurHash3.hash(rowHashes[vRowId - startOffset], Util.DEFAULT_UPDATE_SEED)[0] >>> 1;
              }
              for (int i = 0; i < endOffset - startOffset; i++) {
                if (offsetsLeft == 0) { // if we have offets left from previous time grain processing
                  offsets = offsetFetcher.getRowVector();
                  offsetsLeft = offsetFetcher.getCurrentVectorSize();
                  offsetIdx = 0;
                }
                memory.putLong((long) offsets[offsetIdx++] * Long.BYTES, finalHashes[i]);
                offsetsLeft--;
              }
              rawHashHeapQuickSelectSketch.updateHashes(finalHashes, 0, endOffset - startOffset);

              if (!vectorCursorGranularizer.advanceCursorWithinBucket()) {
                // run the 2nd pass group-by for this grain now
                break;
              }
            } else {
              break;
            }
          }
          grainResultSequences.add(prepareAndRunGroupByQueryForSecondPass(
              query,
              storageAdapter,
              memory,
              rawHashHeapQuickSelectSketch,
              currentInterval,
              bufferHolder
          ));
        }
        return new ConcatSequence<>(Sequences.simple(grainResultSequences.build()));
      }
      catch (Throwable e) {
        try {
          cursor.close();
        }
        catch (Throwable e2) {
          e.addSuppressed(e2);
        }
        throw e;
      }
    }
    catch (Throwable e) {
      bufferHolder.close();
      throw e;
    }
  }

  private static Sequence<ResultRow> prepareAndRunGroupByQueryForSecondPass(
      SamplingGroupByQuery query,
      StorageAdapter storageAdapter,
      WritableMemory memory,
      RawHashHeapQuickSelectSketch rawHashHeapQuickSelectSketch,
      Interval groupingInterval,
      ResourceHolder<ByteBuffer> bufferHolder
  )
  {
    String dimensionHashVirtualColumnName = SamplingGroupByQuery.INTERMEDIATE_RESULT_ROW_HASH_DIMENSION_NAME;
    String thetaVirtualColumnName = SamplingGroupByQuery.INTERMEDIATE_RESULT_ROW_THETA_DIMENSION_NAME;
    DimensionHashVirtualColumn dimensionHashVirtualColumn = new DimensionHashVirtualColumn(
        memory,
        dimensionHashVirtualColumnName
    );

    // get the theta for sampling
    long theta = rawHashHeapQuickSelectSketch.compact().getThetaLong();

    // create a virtual column over theta and a filter to remove rows with hashes > theta
    ExpressionVirtualColumn thetaVirtualColumn = new ExpressionVirtualColumn(
        thetaVirtualColumnName,
        ExprEval.of(theta).toExpr(),
        ColumnType.LONG
    );
    BoundDimFilter boundDimFilter = new BoundDimFilter(
        dimensionHashVirtualColumnName,
        String.valueOf(0),
        String.valueOf(theta),
        true,
        true,
        false,
        null,
        StringComparators.NUMERIC,
        null
    );

    // construct the group by query for 2nd pass
    AndDimFilter andDimFilter = new AndDimFilter(query.getFilter(), boundDimFilter);
    GroupByQuery groupByQuery = query
        .generateIntermediateGroupByQuery()
        .withDimFilter(andDimFilter)
        .withVirtualColumns(VirtualColumns.create(ImmutableList.of(dimensionHashVirtualColumn, thetaVirtualColumn)));

    ByteBuffer groupByBuffer = bufferHolder.get().duplicate();
    groupByBuffer.position(getMaxHashBytes(storageAdapter));
    groupByBuffer = groupByBuffer.slice().order(ByteOrder.nativeOrder());
    return vectorizedSecondPass(
        storageAdapter,
        groupByQuery.getFilter() == null ? null : groupByQuery.getFilter().toFilter(),
        groupingInterval,
        groupByQuery,
        new GroupByQueryConfig(),
        groupByBuffer
    );
  }

  private static Sequence<ResultRow> vectorizedSecondPass(
      StorageAdapter storageAdapter,
      Filter filter,
      Interval interval,
      GroupByQuery query,
      GroupByQueryConfig querySpecificConfig,
      ByteBuffer groupByBuffer
  )
  {
    Map<String, Object> context = new HashMap<>(query.getContext());
    context.put(QueryContexts.VECTORIZE_VIRTUAL_COLUMNS_KEY, QueryContexts.Vectorize.TRUE.toString());
    Sequence<ResultRow> resultRowSequence = VectorGroupByEngine.process(
        query.withOverriddenContext(context),
        storageAdapter,
        groupByBuffer,
        null,
        filter,
        interval,
        querySpecificConfig,
        null // this is bad
    );
    // sort the groups by time, groupHash, grouping dimensions
    List<ResultRow> resultGroups = resultRowSequence.toList();
    resultGroups.sort((Comparator<ResultRow>) query.getResultOrdering());
    return Sequences.simple(resultGroups);
  }

  private static int getMaxHashBytes(StorageAdapter adapter)
  {
    return adapter.getNumRows() * Long.BYTES;
  }
}
