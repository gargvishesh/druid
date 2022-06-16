/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.query.samplinggroupby.hashing;

import org.apache.datasketches.Util;
import org.apache.datasketches.hash.MurmurHash3;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.segment.vector.SingleValueDimensionVectorSelector;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;

public class HashingVectorColumnProcessors
{
  // hashing -1 as a proxy for hashing nulls and empty strings
  public static final long NULL_EMPTY_HASH = MurmurHash3.hash(-1, Util.DEFAULT_UPDATE_SEED)[0] >>> 1;

  /**
   * Vector hasher for string column with known cardinality. Provides supplier for a long vector which works on the
   * current vector of the SingleValueDimensionVectorSelector. Also, caches the hashes using an array where the index of
   * the array represents the dictionary id for the column value.
   */
  public static class CachingStringDimWithCardinalityHasher implements HashVectorSupplier
  {
    private final SingleValueDimensionVectorSelector selector;
    private final long[] dictionaryIdToHash;
    private final boolean supportsLookupNameUtf8;

    public CachingStringDimWithCardinalityHasher(SingleValueDimensionVectorSelector selector)
    {
      this.selector = selector;
      this.dictionaryIdToHash = new long[selector.getValueCardinality()];
      Arrays.fill(dictionaryIdToHash, -1);
      this.supportsLookupNameUtf8 = selector.supportsLookupNameUtf8();
      if (!selector.nameLookupPossibleInAdvance()) {
        throw new ISE("Dictionary lookup is necessary for caching the hashes");
      }
    }

    @Override
    public long[] getHashes(int startOffet, int endOffset)
    {
      int[] vector = selector.getRowVector();
      int vSize = selector.getCurrentVectorSize();
      long[] resultHashes = new long[vSize];
      if (supportsLookupNameUtf8) {
        for (int vRowId = startOffet; vRowId < endOffset; vRowId++) {
          long hashVal = dictionaryIdToHash[vector[vRowId]];
          if (hashVal != -1) {
            resultHashes[vRowId - startOffet] = hashVal;
            continue;
          }
          ByteBuffer value = selector.lookupNameUtf8(vector[vRowId]);
          if (value != null && value.hasRemaining()) {
            byte[] bytes = new byte[value.remaining()];
            value.mark();
            value.get(bytes);
            value.reset();
            hashVal = MurmurHash3.hash(bytes, Util.DEFAULT_UPDATE_SEED)[0] >>> 1;
          } else {
            hashVal = NULL_EMPTY_HASH;
          }
          dictionaryIdToHash[vector[vRowId]] = hashVal;
          resultHashes[vRowId - startOffet] = hashVal;
        }
      } else {
        for (int vRowId = startOffet; vRowId < endOffset; vRowId++) {
          long hashVal = dictionaryIdToHash[vector[vRowId]];
          if (hashVal != -1) {
            resultHashes[vRowId - startOffet] = hashVal;
            continue;
          }
          String value = selector.lookupName(vector[vRowId]);
          if (value != null && !value.isEmpty()) {
            hashVal = MurmurHash3.hash(value.getBytes(StandardCharsets.UTF_8), Util.DEFAULT_UPDATE_SEED)[0] >>> 1;
          } else {
            hashVal = NULL_EMPTY_HASH;
          }
          dictionaryIdToHash[vector[vRowId]] = hashVal;
          resultHashes[vRowId - startOffet] = hashVal;
        }
      }
      return resultHashes;
    }
  }

  /**
   * Vector hasher for string column with unknown cardinality. Provides supplier for a long vector which works on the
   * current vector of the SingleValueDimensionVectorSelector.
   */
  public static class StringDimWithoutCardinalityHasher implements HashVectorSupplier
  {
    private final SingleValueDimensionVectorSelector selector;
    private final boolean supportsLookupNameUtf8;

    public StringDimWithoutCardinalityHasher(SingleValueDimensionVectorSelector selector)
    {
      this.selector = selector;
      this.supportsLookupNameUtf8 = selector.supportsLookupNameUtf8();
    }

    @Override
    public long[] getHashes(int startOffet, int endOffset)
    {
      int[] vector = selector.getRowVector();
      int vSize = selector.getCurrentVectorSize();
      long[] resultHashes = new long[vSize];
      if (supportsLookupNameUtf8) {
        for (int vRowId = startOffet; vRowId < endOffset; vRowId++) {
          ByteBuffer value = selector.lookupNameUtf8(vector[vRowId]);
          if (value != null && value.hasRemaining()) {
            byte[] bytes = new byte[value.remaining()];
            value.mark();
            value.get(bytes);
            value.reset();
            resultHashes[vRowId - startOffet] = MurmurHash3.hash(bytes, Util.DEFAULT_UPDATE_SEED)[0] >>> 1;
          } else {
            resultHashes[vRowId - startOffet] = NULL_EMPTY_HASH;
          }
        }
      } else {
        for (int vRowId = startOffet; vRowId < endOffset; vRowId++) {
          String value = selector.lookupName(vector[vRowId]);
          if (value != null && !value.isEmpty()) {
            resultHashes[vRowId - startOffet] = MurmurHash3.hash(
                value.getBytes(StandardCharsets.UTF_8),
                Util.DEFAULT_UPDATE_SEED
            )[0] >>> 1;
          } else {
            resultHashes[vRowId - startOffet] = NULL_EMPTY_HASH;
          }
        }
      }
      return resultHashes;
    }
  }

  /**
   * Vector hasher for string column with known cardinality. Provides supplier for a long vector which works on the
   * current vector of the SingleValueDimensionVectorSelector. Also, caches the hashes using a bounder map where the key
   * of the map entry represents the dictionary id for the column value and the value of the map entry represents the
   * hash value of the column value.
   */
  public static class CachingStringDimWithoutCardinalityHasher implements HashVectorSupplier
  {
    private final SingleValueDimensionVectorSelector selector;
    private final boolean supportsLookupNameUtf8;
    private final Map<Integer, Long> dictIdToHashCache;

    public CachingStringDimWithoutCardinalityHasher(SingleValueDimensionVectorSelector selector, int maxCacheSize)
    {
      this.selector = selector;
      this.supportsLookupNameUtf8 = selector.supportsLookupNameUtf8();
      if (!selector.nameLookupPossibleInAdvance()) {
        throw new ISE("Dictionary lookup is necessary for caching the hashes");
      }
      dictIdToHashCache = new LinkedHashMap<Integer, Long>()
      {
        @Override
        protected boolean removeEldestEntry(Map.Entry eldest)
        {
          return size() > maxCacheSize;
        }
      };
    }

    @Override
    public long[] getHashes(int startOffet, int endOffset)
    {
      int[] vector = selector.getRowVector();
      int vSize = selector.getCurrentVectorSize();
      long[] resultHashes = new long[vSize];
      if (supportsLookupNameUtf8) {
        for (int vRowId = startOffet; vRowId < endOffset; vRowId++) {
          resultHashes[vRowId - startOffet] = dictIdToHashCache.computeIfAbsent(vector[vRowId], dictId -> {
            ByteBuffer value = selector.lookupNameUtf8(dictId);
            if (value != null && value.hasRemaining()) {
              byte[] bytes = new byte[value.remaining()];
              value.mark();
              value.get(bytes);
              value.reset();
              return MurmurHash3.hash(bytes, Util.DEFAULT_UPDATE_SEED)[0] >>> 1;
            } else {
              return NULL_EMPTY_HASH;
            }
          });
        }
      } else {
        for (int vRowId = startOffet; vRowId < endOffset; vRowId++) {
          resultHashes[vRowId - startOffet] = dictIdToHashCache.computeIfAbsent(vector[vRowId], dictId -> {
            String value = selector.lookupName(dictId);
            if (value != null && !value.isEmpty()) {
              return MurmurHash3.hash(value.getBytes(StandardCharsets.UTF_8), Util.DEFAULT_UPDATE_SEED)[0] >>> 1;
            } else {
              return NULL_EMPTY_HASH;
            }
          });
        }
      }
      return resultHashes;
    }
  }
}
