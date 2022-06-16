/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.query.samplinggroupby.engine.onepass;

import com.google.common.collect.ImmutableList;
import io.imply.druid.query.samplinggroupby.SamplingGroupByQuery;
import it.unimi.dsi.fastutil.ints.Int2LongOpenHashMap;
import org.apache.datasketches.Util;
import org.apache.datasketches.theta.HashIterator;
import org.apache.datasketches.theta.RawHashHeapQuickSelectSketch;
import org.apache.druid.annotations.EverythingIsNonnullByDefault;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.java.util.common.parsers.CloseableIterator;
import org.apache.druid.query.ColumnSelectorPlus;
import org.apache.druid.query.aggregation.AggregatorAdapters;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.dimension.DefaultDimensionSpec;
import org.apache.druid.query.groupby.epinephelinae.GroupByQueryEngineV2;
import org.apache.druid.query.groupby.epinephelinae.Grouper;
import org.apache.druid.query.groupby.epinephelinae.TestColumnSelectorFactory;
import org.apache.druid.query.groupby.epinephelinae.column.GroupByColumnSelectorPlus;
import org.apache.druid.query.groupby.epinephelinae.column.GroupByColumnSelectorStrategy;
import org.apache.druid.segment.DimensionHandlerUtils;
import org.apache.druid.segment.column.ColumnCapabilities;
import org.apache.druid.segment.column.ColumnCapabilitiesImpl;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.join.filter.AllNullColumnSelectorFactory;
import org.apache.druid.segment.selector.settable.SettableLongColumnValueSelector;
import org.junit.Assert;
import org.junit.Test;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;

public class SamplingBufferHashGrouperTest
{
  @Test
  public void testIterator()
  {
    NullHandling.initializeForTests();
    int maxGroups = 16;
    int keySize = 24 + (NullHandling.replaceWithDefault() ? 0 : 3);
    RawHashHeapQuickSelectSketch rawHashHeapQuickSelectSketch = RawHashHeapQuickSelectSketch.create(
        maxGroups,
        Util.DEFAULT_UPDATE_SEED
    );
    RawHashHeapQuickSelectSketch expectedRawHashHeapQuickSelectSketch = RawHashHeapQuickSelectSketch.create(
        maxGroups,
        Util.DEFAULT_UPDATE_SEED
    );

    ColumnSelectorPlus<GroupByColumnSelectorStrategy>[] selectorPlus = DimensionHandlerUtils
        .createColumnSelectorPluses(
            GroupByQueryEngineV2.STRATEGY_FACTORY,
            ImmutableList.of(
                new DefaultDimensionSpec(
                    SamplingGroupByQuery.INTERMEDIATE_RESULT_ROW_HASH_DIMENSION_NAME,
                    SamplingGroupByQuery.INTERMEDIATE_RESULT_ROW_HASH_DIMENSION_NAME,
                    ColumnType.LONG
                ),
                new DefaultDimensionSpec(
                    "dummy",
                    "dummy",
                    ColumnType.LONG
                ),
                new DefaultDimensionSpec(
                    SamplingGroupByQuery.INTERMEDIATE_RESULT_ROW_THETA_DIMENSION_NAME,
                    SamplingGroupByQuery.INTERMEDIATE_RESULT_ROW_THETA_DIMENSION_NAME,
                    ColumnType.LONG
                )
            ),
            new TestLongColumnSelectorFactory()
        );
    GroupByColumnSelectorPlus[] dims = GroupByQueryEngineV2.createGroupBySelectorPlus(
        selectorPlus,
        0
    );
    SamplingBufferHashGrouper samplingBufferHashGrouper = new SamplingBufferHashGrouper(
        () -> ByteBuffer.allocate(5000),
        new TestKeySerde(keySize),
        dims,
        AggregatorAdapters.factorizeBuffered(new AllNullColumnSelectorFactory(), ImmutableList.of()),
        Integer.MAX_VALUE,
        4,
        0.9F,
        new Int2LongOpenHashMap(),
        rawHashHeapQuickSelectSketch,
        maxGroups
    );
    samplingBufferHashGrouper.init();

    SettableLongColumnValueSelector settableLongColumnValueSelector = new SettableLongColumnValueSelector();
    for (int i = 0; i < 35; i++) {
      samplingBufferHashGrouper.setCurrRowGroupHash(SamplingByteBufferHashTableTest.getRawHash(i));
      ByteBuffer keyBuffer = ByteBuffer.allocate(keySize);
      settableLongColumnValueSelector.setValue(i);
      dims[1].getColumnSelectorStrategy().writeToKeyBuffer(
          dims[1].getKeyBufferPosition(),
          settableLongColumnValueSelector,
          keyBuffer
      );
      Assert.assertTrue(
          samplingBufferHashGrouper.aggregate(keyBuffer,
          SamplingByteBufferHashTableTest.getAggregateHash(i)).isOk()
      );
      expectedRawHashHeapQuickSelectSketch.updateHash(SamplingByteBufferHashTableTest.getRawHash(i));
    }
    int count = 0;
    HashIterator iterator = expectedRawHashHeapQuickSelectSketch.compact().iterator();
    long[] expectedRetainedVals = new long[maxGroups];
    long[] returnedRetainedVals = new long[maxGroups];
    for (CloseableIterator<Grouper.Entry<ByteBuffer>> it = samplingBufferHashGrouper.iterator(true); it.hasNext();) {
      Grouper.Entry<ByteBuffer> entry = it.next();
      returnedRetainedVals[count] = entry.getKey().getLong(NullHandling.replaceWithDefault() ? 0 : 1);
      if (iterator.next()) {
        expectedRetainedVals[count++] = iterator.get();
      } else {
        throw new RuntimeException("Unexpected output");
      }
    }
    Assert.assertEquals(maxGroups, count); // this comes 16 with 35, but 18 with 40. some sketch semantics.
    Arrays.sort(returnedRetainedVals);
    Assert.assertArrayEquals(expectedRetainedVals, returnedRetainedVals);
    Assert.assertEquals(rawHashHeapQuickSelectSketch.compact().getThetaLong(), samplingBufferHashGrouper.getCurrTheta());
  }

  @EverythingIsNonnullByDefault
  private static class TestKeySerde implements Grouper.KeySerde<ByteBuffer>
  {
    private final int keySize;

    public TestKeySerde(int keySize)
    {
      this.keySize = keySize;
    }

    @Override
    public int keySize()
    {
      return keySize;
    }

    @Override
    public Class<ByteBuffer> keyClazz()
    {
      return ByteBuffer.class;
    }

    @Override
    public List<String> getDictionary()
    {
      return ImmutableList.of();
    }

    @Nullable
    @Override
    public ByteBuffer toByteBuffer(ByteBuffer key)
    {
      return key;
    }

    @Override
    public ByteBuffer createKey()
    {
      return ByteBuffer.allocate(keySize());
    }

    @Override
    public void readFromByteBuffer(ByteBuffer key, ByteBuffer buffer, int position)
    {
      ByteBuffer dup = buffer.duplicate();
      dup.position(position).limit(position + keySize());
      key.mark();
      key.put(dup);
      key.reset();
    }

    @Override
    public Grouper.BufferComparator bufferComparator()
    {
      return ((lhsBuffer, rhsBuffer, lhsPosition, rhsPosition) ->
          Integer.compare(lhsBuffer.getInt(lhsPosition + 8), rhsBuffer.getInt(rhsPosition + 8)));
    }

    @Override
    public Grouper.BufferComparator bufferComparatorWithAggregators(
        AggregatorFactory[] aggregatorFactories,
        int[] aggregatorOffsets
    )
    {
      return bufferComparator();
    }

    @Override
    public void reset()
    {

    }
  }

  private static class TestLongColumnSelectorFactory extends TestColumnSelectorFactory
  {
    @Override
    public ColumnCapabilities getColumnCapabilities(String columnName)
    {
      return ColumnCapabilitiesImpl.createSimpleNumericColumnCapabilities(ColumnType.LONG);
    }
  }
}
