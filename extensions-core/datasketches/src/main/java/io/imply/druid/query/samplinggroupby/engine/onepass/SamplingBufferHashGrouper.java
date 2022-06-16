/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.query.samplinggroupby.engine.onepass;

import com.google.common.base.Supplier;
import com.google.common.collect.Iterators;
import com.google.common.collect.PeekingIterator;
import it.unimi.dsi.fastutil.ints.Int2LongMap;
import it.unimi.dsi.fastutil.ints.Int2LongOpenHashMap;
import org.apache.datasketches.theta.RawHashHeapQuickSelectSketch;
import org.apache.druid.java.util.common.CloseableIterators;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.parsers.CloseableIterator;
import org.apache.druid.query.aggregation.AggregatorAdapters;
import org.apache.druid.query.groupby.epinephelinae.AbstractBufferHashGrouper;
import org.apache.druid.query.groupby.epinephelinae.AggregateResult;
import org.apache.druid.query.groupby.epinephelinae.ByteBufferHashTable;
import org.apache.druid.query.groupby.epinephelinae.ReusableEntry;
import org.apache.druid.query.groupby.epinephelinae.column.GroupByColumnSelectorPlus;
import org.apache.druid.segment.selector.settable.SettableLongColumnValueSelector;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.NotThreadSafe;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Comparator;
import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicReference;

@NotThreadSafe
public class SamplingBufferHashGrouper extends AbstractBufferHashGrouper<ByteBuffer>
{
  private static final float DEFAULT_MAX_LOAD_FACTOR = 0.75f;
  private static final int MIN_INITIAL_BUCKETS = 4;
  private static final int DEFAULT_INITIAL_BUCKETS = 1024;
  private final GroupByColumnSelectorPlus[] dims;
  private final Int2LongOpenHashMap groupHashes;
  private final RawHashHeapQuickSelectSketch rawHashHeapQuickSelectSketch;
  private boolean initialized;
  private long currRowGroupHash;
  private final AtomicReference<Long> currThetaRef;
  private final int maxGroups;
  private final SettableLongColumnValueSelector settableLongColumnValueSelector;

  public SamplingBufferHashGrouper(
      Supplier<ByteBuffer> bufferSupplier,
      KeySerde<ByteBuffer> keySerde,
      GroupByColumnSelectorPlus[] dims,
      AggregatorAdapters aggregators,
      int bufferGrouperMaxSize,
      int initialBucket,
      float maxLoadFactor,
      Int2LongOpenHashMap groupHashes,
      RawHashHeapQuickSelectSketch rawHashHeapQuickSelectSketch,
      int maxGroups
  )
  {
    super(bufferSupplier, keySerde, aggregators, HASH_SIZE + keySerde.keySize(), bufferGrouperMaxSize);
    this.dims = dims;
    this.groupHashes = groupHashes;
    this.maxLoadFactor = maxLoadFactor;
    this.rawHashHeapQuickSelectSketch = rawHashHeapQuickSelectSketch;
    this.bucketSize = HASH_SIZE + keySerde.keySize() + aggregators.spaceNeeded();
    this.maxLoadFactor = maxLoadFactor > 0 ? maxLoadFactor : DEFAULT_MAX_LOAD_FACTOR;
    this.initialBuckets = initialBucket > 0 ? Math.max(MIN_INITIAL_BUCKETS, initialBucket) : DEFAULT_INITIAL_BUCKETS;
    this.currThetaRef = new AtomicReference<>(rawHashHeapQuickSelectSketch.getThetaLong());
    this.maxGroups = maxGroups;
    this.settableLongColumnValueSelector = new SettableLongColumnValueSelector();

    if (this.maxLoadFactor >= 1.0f) {
      throw new IAE("Invalid maxLoadFactor[%f], must be < 1.0", maxLoadFactor);
    }
  }

  @Override
  public void newBucketHook(int bucketOffset)
  {
    groupHashes.put(bucketOffset, currRowGroupHash);
    rawHashHeapQuickSelectSketch.updateHash(currRowGroupHash);
  }

  @Override
  public boolean canSkipAggregate(int bucketOffset)
  {
    return false;
  }

  @Override
  public void afterAggregateHook(int bucketOffset)
  {
    // Nothing to do
  }

  @Override
  public void init()
  {
    // Copy : from BufferHashGrouper except injecting our custom sampling hash table
    if (!initialized) {
      ByteBuffer buffer = bufferSupplier.get();

      int hashTableSize = ByteBufferHashTable.calculateTableArenaSizeWithPerBucketAdditionalSize(
          buffer.capacity(),
          bucketSize,
          Integer.BYTES
      );

      hashTableBuffer = buffer.duplicate();
      hashTableBuffer.position(0);
      hashTableBuffer.limit(hashTableSize);
      hashTableBuffer = hashTableBuffer.slice();

      SamplingBufferGrouperBucketUpdateHandler samplingBufferGrouperBucketUpdateHandler =
          new SamplingBufferGrouperBucketUpdateHandler();
      this.hashTable = new SamplingByteBufferHashTable(
          maxLoadFactor,
          initialBuckets,
          bucketSize,
          hashTableBuffer,
          keySize,
          bufferGrouperMaxSize,
          samplingBufferGrouperBucketUpdateHandler,
          groupHashes,
          rawHashHeapQuickSelectSketch,
          maxGroups,
          currentTheta -> {
            currThetaRef.set(currentTheta);
            samplingBufferGrouperBucketUpdateHandler.finishIter();
          }
      );

      reset();
      initialized = true;
    }
  }

  @Override
  public boolean isInitialized()
  {
    return initialized;
  }

  @Override
  public void reset()
  {
    hashTable.reset();
    groupHashes.clear();
  }

  @Override
  @Nonnull
  public AggregateResult aggregate(@Nonnull ByteBuffer key)
  {
    throw new UnsupportedOperationException("Sampling grouper doesn't support aggregation without providing hash");
  }

  @Override
  @Nonnull
  public CloseableIterator<Entry<ByteBuffer>> iterator(boolean sorted)
  {
    // Copy : The if condition below is picked from BufferHashGrouper
    if (!initialized) {
      // it's possible for iterator() to be called before initialization when
      // a nested groupBy's subquery has an empty result set (see testEmptySubquery() in GroupByQueryRunnerTest)
      return CloseableIterators.withEmptyBaggage(Collections.emptyIterator());
    }

    long finalTheta = rawHashHeapQuickSelectSketch.compact().getThetaLong();
    currThetaRef.set(finalTheta);
    return new CloseableIterator<Entry<ByteBuffer>>()
    {
      final PeekingIterator<Int2LongMap.Entry> sortedGroupIterator = Iterators.peekingIterator(
          groupHashes
          .int2LongEntrySet()
          .stream()
          .sorted(Comparator.comparingLong(Int2LongMap.Entry::getLongValue))
          .iterator()
      );
      Entry<ByteBuffer> nextEntry;
      final ReusableEntry<ByteBuffer> reusableEntry = ReusableEntry.create(keySerde, aggregators.size());

      @Override
      public void close()
      {
        // Nothing to do
      }

      @Override
      public boolean hasNext()
      {
        return sortedGroupIterator.hasNext() && sortedGroupIterator.peek().getLongValue() < finalTheta;
      }

      @Override
      public Entry<ByteBuffer> next()
      {
        if (!hasNext()) {
          throw new NoSuchElementException();
        }
        while (sortedGroupIterator.hasNext()) {
          Int2LongMap.Entry groupHashEntry = sortedGroupIterator.next();
          if (groupHashEntry.getLongValue() < finalTheta) {
            nextEntry = populateBucketEntryForOffset(reusableEntry, groupHashEntry.getIntKey());
            // hash is at first position
            settableLongColumnValueSelector.setValue(groupHashEntry.getLongValue());
            dims[0].getColumnSelectorStrategy().writeToKeyBuffer(
                dims[0].getKeyBufferPosition(),
                settableLongColumnValueSelector,
                nextEntry.getKey()
            );
            // theta is at last position
            settableLongColumnValueSelector.setValue(finalTheta);
            dims[dims.length - 1].getColumnSelectorStrategy().writeToKeyBuffer(
                dims[dims.length - 1].getKeyBufferPosition(),
                settableLongColumnValueSelector,
                nextEntry.getKey()
            );
            return nextEntry;
          }
        }
        throw new ISE("Expected an element in the iterator");
      }
    };
  }

  // HACK : set this hash so that our groupHashes map can get updated for all new buckets
  // we could've used the Grouper#aggregate(key, hash) interface but that takes an integer hash and groupHashes has long hashes
  public void setCurrRowGroupHash(long currRowGroupHash)
  {
    this.currRowGroupHash = currRowGroupHash;
  }

  public long getCurrTheta()
  {
    return currThetaRef.get();
  }

  private class SamplingBufferGrouperBucketUpdateHandler implements ByteBufferHashTable.BucketUpdateHandler
  {
    private final Int2LongOpenHashMap newGroupHashes;

    public SamplingBufferGrouperBucketUpdateHandler()
    {
      newGroupHashes = new Int2LongOpenHashMap();
    }

    @Override
    public void handleNewBucket(int bucketOffset)
    {
      // Nothing to do
    }

    @Override
    public void handlePreTableSwap()
    {
      // Nothing to do
    }

    @Override
    public void handleBucketMove(
        int oldBucketOffset,
        int newBucketOffset,
        @Nonnull ByteBuffer oldBuffer,
        @Nonnull ByteBuffer newBuffer
    )
    {
      // Copy : from BufferHashGrouper
      // relocate aggregators (see https://github.com/apache/druid/pull/4071)
      aggregators.relocate(
          oldBucketOffset + baseAggregatorOffset,
          newBucketOffset + baseAggregatorOffset,
          oldBuffer,
          newBuffer
      );
      newGroupHashes.put(newBucketOffset, groupHashes.get(oldBucketOffset));
    }

    public void finishIter()
    {
      groupHashes.clear();
      groupHashes.putAll(newGroupHashes);
      newGroupHashes.clear();
    }
  }
}
