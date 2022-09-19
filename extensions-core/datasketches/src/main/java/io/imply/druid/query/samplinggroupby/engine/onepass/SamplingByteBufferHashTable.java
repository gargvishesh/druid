/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.query.samplinggroupby.engine.onepass;

import it.unimi.dsi.fastutil.ints.Int2LongOpenHashMap;
import org.apache.datasketches.theta.RawHashHeapQuickSelectSketch;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.query.groupby.epinephelinae.ByteBufferHashTable;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.util.function.Consumer;

public class SamplingByteBufferHashTable extends ByteBufferHashTable
{
  private final RawHashHeapQuickSelectSketch rawHashHeapQuickSelectSketch;
  private final Int2LongOpenHashMap groupHashes; // byteBufferOffset of hashtable -> groupHash map
  private final Consumer<Long> resizeFinishedConsumer;
  private final int maxGroups;

  public SamplingByteBufferHashTable(
      float maxLoadFactor,
      int initialBuckets,
      int bucketSizeWithHash,
      ByteBuffer buffer,
      int keySize,
      int maxSizeForTesting,
      @Nullable BucketUpdateHandler bucketUpdateHandler,
      Int2LongOpenHashMap groupHashes,
      RawHashHeapQuickSelectSketch rawHashHeapQuickSelectSketch,
      int maxGroups,
      Consumer<Long> resizeFinishedConsumer
  )
  {
    super(maxLoadFactor, initialBuckets, bucketSizeWithHash, buffer, keySize, maxSizeForTesting, bucketUpdateHandler);
    this.groupHashes = groupHashes;
    this.rawHashHeapQuickSelectSketch = rawHashHeapQuickSelectSketch;
    this.maxGroups = maxGroups;
    this.resizeFinishedConsumer = resizeFinishedConsumer;
  }

  @Override
  public void adjustTableWhenFull()
  {
    // filtering groups which are greater than theta while resizing the table by marking them as unused
    long currentTheta = rawHashHeapQuickSelectSketch.compact().getThetaLong();
    int newBuckets = 0;
    for (int i = 0; i < maxBuckets; i++) {
      if (!isBucketUsed(i)) {
        continue;
      }
      if (!groupHashes.containsKey(i * bucketSizeWithHash)) {
        throw new ISE("Sampling hash of group %d is missing", i * bucketSizeWithHash);
      }
      if (groupHashes.get(i * bucketSizeWithHash) >= currentTheta) {
        tableBuffer.put(i * bucketSizeWithHash, (byte) 0);
        groupHashes.remove(i * bucketSizeWithHash); // clean up groupHashes map too
        size--;
      } else {
        newBuckets++;
      }
    }

    // Copy : From ByteBufferHashTable except from some assertions

    newBuckets = Math.min(newBuckets * 2, 2 * maxGroups);
    newBuckets = (int) Math.ceil(newBuckets * 1D / maxLoadFactor);

    if (tableStart == 0) {
      // tableStart = 0 is the last growth; no further growing is possible.
      return;
    }

    int newMaxSize;
    int newTableStart;

    if (((long) (maxBuckets + newBuckets) * bucketSizeWithHash) > (long) tableArenaSize - tableStart) {
      // Not enough space to grow upwards, start back from zero
      newTableStart = 0;
      // the new buckets will also be bounded by the remaining size of the buffer, ie [0, tableStart) bytes
      newBuckets = Math.min(newBuckets, tableStart / bucketSizeWithHash);
    } else {
      newTableStart = tableStart + tableBuffer.limit();
    }
    newMaxSize = maxSizeForBuckets(newBuckets);

    ByteBuffer newTableBuffer = buffer.duplicate();
    newTableBuffer.position(newTableStart);
    newTableBuffer.limit(newTableStart + newBuckets * bucketSizeWithHash);
    newTableBuffer = newTableBuffer.slice();

    int newSize = 0;

    // Clear used bits of new table
    for (int i = 0; i < newBuckets; i++) {
      newTableBuffer.put(i * bucketSizeWithHash, (byte) 0);
    }

    // Loop over old buckets and copy to new table
    final ByteBuffer entryBuffer = tableBuffer.duplicate();
    final ByteBuffer keyBuffer = tableBuffer.duplicate();

    int oldBuckets = maxBuckets;

    if (bucketUpdateHandler != null) {
      bucketUpdateHandler.handlePreTableSwap();
    }

    for (int oldBucket = 0; oldBucket < oldBuckets; oldBucket++) {
      if (isBucketUsed(oldBucket)) {
        int oldBucketOffset = oldBucket * bucketSizeWithHash;
        entryBuffer.limit((oldBucket + 1) * bucketSizeWithHash);
        entryBuffer.position(oldBucketOffset);
        keyBuffer.limit(entryBuffer.position() + HASH_SIZE + keySize);
        keyBuffer.position(entryBuffer.position() + HASH_SIZE);

        final int keyHash = entryBuffer.getInt(entryBuffer.position()) & 0x7fffffff;
        final int newBucket = findBucket(true, newBuckets, newTableBuffer, keyBuffer, keyHash);

        if (newBucket < 0) {
          throw new ISE("Couldn't find a bucket while resizing");
        }

        final int newBucketOffset = newBucket * bucketSizeWithHash;

        newTableBuffer.position(newBucketOffset);
        newTableBuffer.put(entryBuffer);

        newSize++;

        if (bucketUpdateHandler != null) {
          bucketUpdateHandler.handleBucketMove(oldBucketOffset, newBucketOffset, tableBuffer, newTableBuffer);
        }
      }
    }

    maxBuckets = newBuckets;
    regrowthThreshold = newMaxSize;
    tableBuffer = newTableBuffer;
    tableStart = newTableStart;

    growthCount++;
    resizeFinishedConsumer.accept(currentTheta);

    if (size != newSize) {
      throw new ISE("size[%,d] != newSize[%,d] after resizing", size, newSize);
    }
  }

  @Override
  public void reset()
  {
    super.reset();
    groupHashes.clear();
  }

  @Override
  protected int findBucketWithAutoGrowth(@Nonnull ByteBuffer keyBuffer, int keyHash, @Nonnull Runnable preTableGrowthRunnable)
  {
    return super.findBucketWithAutoGrowth(keyBuffer, keyHash, preTableGrowthRunnable);
  }

  @Override
  protected void initializeNewBucketKey(int bucket, @Nonnull ByteBuffer keyBuffer, int keyHash)
  {
    super.initializeNewBucketKey(bucket, keyBuffer, keyHash);
  }

  @Override
  public int getSize()
  {
    return super.getSize();
  }
}
