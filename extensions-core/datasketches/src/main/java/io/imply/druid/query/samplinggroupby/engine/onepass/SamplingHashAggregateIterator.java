
/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.query.samplinggroupby.engine.onepass;

import com.google.common.base.Suppliers;
import io.imply.druid.query.samplinggroupby.SamplingGroupByQuery;
import io.imply.druid.query.samplinggroupby.hashing.HashSupplier;
import io.imply.druid.query.samplinggroupby.hashing.HashingColumnProcessorFactory;
import it.unimi.dsi.fastutil.ints.Int2LongOpenHashMap;
import org.apache.datasketches.Util;
import org.apache.datasketches.hash.MurmurHash3;
import org.apache.datasketches.theta.RawHashHeapQuickSelectSketch;
import org.apache.druid.annotations.EverythingIsNonnullByDefault;
import org.apache.druid.query.aggregation.AggregatorAdapters;
import org.apache.druid.query.groupby.GroupByQueryConfig;
import org.apache.druid.query.groupby.ResultRow;
import org.apache.druid.query.groupby.epinephelinae.GroupByQueryEngineV2;
import org.apache.druid.query.groupby.epinephelinae.Grouper;
import org.apache.druid.query.groupby.epinephelinae.column.GroupByColumnSelectorPlus;
import org.apache.druid.query.groupby.epinephelinae.column.GroupByColumnSelectorStrategy;
import org.apache.druid.segment.ColumnProcessors;
import org.apache.druid.segment.ColumnSelectorFactory;
import org.apache.druid.segment.Cursor;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.function.Supplier;
import java.util.stream.Collectors;

@EverythingIsNonnullByDefault
public class SamplingHashAggregateIterator extends GroupByQueryEngineV2.HashAggregateIterator
{
  private final int maxGroups;
  private final RawHashHeapQuickSelectSketch rawHashHeapQuickSelectSketch;
  private final Supplier<Long> hashSupplier;
  private long currTheta;

  public SamplingHashAggregateIterator(
      SamplingGroupByQuery query,
      GroupByQueryConfig querySpecificConfig,
      Cursor cursor,
      ByteBuffer buffer,
      GroupByColumnSelectorPlus[] dims,
      boolean allSingleValueDims,
      int maxGroups
  )
  {
    super(query.generateIntermediateGroupByQuery(), querySpecificConfig, cursor, buffer, null, dims, allSingleValueDims);
    this.maxGroups = maxGroups;
    this.rawHashHeapQuickSelectSketch = RawHashHeapQuickSelectSketch.create(Math.max(maxGroups, 16), Util.DEFAULT_UPDATE_SEED);
    this.currTheta = rawHashHeapQuickSelectSketch.compact().getThetaLong();

    ColumnSelectorFactory columnSelectorFactory = cursor.getColumnSelectorFactory();
    List<HashSupplier> hashSelectors = query
        .getDimensions()
        .stream()
        .map(dimensionSpec -> ColumnProcessors.makeProcessor(
            dimensionSpec,
            HashingColumnProcessorFactory.instance(),
            columnSelectorFactory
        ))
        .collect(Collectors.toList());
    long[] rowHashes = new long[hashSelectors.size() + 1];
    rowHashes[hashSelectors.size()] =
        query.isIntermediateResultRowWithTimestamp() ?
        MurmurHash3.hash(cursor.getTime().getMillis(), Util.DEFAULT_UPDATE_SEED)[0] >>> 1 : 0;
    this.hashSupplier = () -> {
      for (int i = 0; i < hashSelectors.size(); i++) {
        rowHashes[i] = hashSelectors.get(i).getHash();
      }
      return MurmurHash3.hash(rowHashes, Util.DEFAULT_UPDATE_SEED)[0] >>> 1;
    };
  }

  @Override
  protected Grouper<ByteBuffer> newGrouper()
  {
    return new SamplingBufferHashGrouper(
      Suppliers.ofInstance(buffer),
      keySerde,
      dims,
      AggregatorAdapters.factorizeBuffered(
          cursor.getColumnSelectorFactory(),
          query.getAggregatorSpecs()
      ),
      querySpecificConfig.getBufferGrouperMaxSize(),
      querySpecificConfig.getBufferGrouperInitialBuckets(),
      querySpecificConfig.getBufferGrouperMaxLoadFactor(),
      new Int2LongOpenHashMap(2 * maxGroups),
      rawHashHeapQuickSelectSketch,
      maxGroups
    );
  }

  @Override
  protected void aggregateSingleValueDims(Grouper<ByteBuffer> grouper)
  {
    SamplingBufferHashGrouper samplingBufferHashGrouper = (SamplingBufferHashGrouper) grouper;
    while (!cursor.isDone()) {
      Long hashVal = hashSupplier.get();
      if (hashVal <= currTheta) {
        // TODO : should we add the feature for returning partial results when dictionary grows beyond a limit
        // One advantage is that we're filtering some rows
        for (GroupByColumnSelectorPlus dim : dims) {
          GroupByColumnSelectorStrategy strategy = dim.getColumnSelectorStrategy();
          strategy.writeToKeyBuffer(
              dim.getKeyBufferPosition(),
              dim.getSelector(),
              keyBuffer
          );
        }
        keyBuffer.rewind();

        samplingBufferHashGrouper.setCurrRowGroupHash(hashVal);
        if (!samplingBufferHashGrouper.aggregate(keyBuffer, hashVal.intValue() >>> 1).isOk()) {
          return;
        }
        currTheta = samplingBufferHashGrouper.getCurrTheta();
      }
      cursor.advance();
    }
  }

  @Override
  protected void aggregateMultiValueDims(Grouper<ByteBuffer> grouper)
  {
    throw new UnsupportedOperationException(
        "Multi Valued Dimensions are not supported as grouping columns in "
        + SamplingGroupByQuery.QUERY_TYPE + " query");
  }

  @Override
  protected void putToRow(ByteBuffer key, ResultRow resultRow)
  {
    for (GroupByColumnSelectorPlus selectorPlus : dims) {
      selectorPlus.getColumnSelectorStrategy().processValueFromGroupingKey(
          selectorPlus,
          key,
          resultRow,
          selectorPlus.getKeyBufferPosition()
      );
    }
  }
}
