/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.query.samplinggroupby.hashing;

import io.imply.druid.query.samplinggroupby.SamplingGroupByQuery;
import org.apache.datasketches.Util;
import org.apache.datasketches.hash.MurmurHash3;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.segment.DimensionSelector;
import org.apache.druid.segment.data.IndexedInts;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;

public class HashingColumnProcessors
{
  /**
   * Hasher for string column with known cardinality. Provides supplier for a long value which works on the current
   * value of the DimensionSelector. Also, caches the hash using an array where the index of the array represents the
   * dictionary id for the column value.
   */
  public static class CachingStringDimWithCardinalityHasher implements HashSupplier
  {
    private final DimensionSelector selector;
    private final long[] dictionaryIdToHash;
    private final boolean supportsLookupNameUtf8;

    public CachingStringDimWithCardinalityHasher(DimensionSelector selector)
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
    public long getHash()
    {
      IndexedInts indexedInts = selector.getRow();
      if (indexedInts.size() == 0) {
        return HashingVectorColumnProcessors.NULL_EMPTY_HASH;
      }
      if (indexedInts.size() > 1) {
        throw new UnsupportedOperationException(
            "Multi Valued Dimensions are not supported as grouping columns in "
            + SamplingGroupByQuery.QUERY_TYPE + " query");
      }

      int dictId = indexedInts.get(0);
      long hashVal = dictionaryIdToHash[dictId];
      if (hashVal != -1) {
        return hashVal;
      }
      if (supportsLookupNameUtf8) { // fast path since their is no conversion from UTF-8 to UTF-16LE (java string encoding)
        ByteBuffer value = selector.lookupNameUtf8(dictId);
        if (value != null && value.hasRemaining()) {
          byte[] bytes = new byte[value.remaining()];
          value.mark();
          value.get(bytes);
          value.reset();
          hashVal = MurmurHash3.hash(bytes, Util.DEFAULT_UPDATE_SEED)[0] >>> 1;
        } else {
          hashVal = HashingVectorColumnProcessors.NULL_EMPTY_HASH;
        }
      } else {
        String value = selector.lookupName(dictId);
        if (value != null && !value.isEmpty()) {
          hashVal = MurmurHash3.hash(value.getBytes(StandardCharsets.UTF_8), Util.DEFAULT_UPDATE_SEED)[0] >>> 1;
        } else {
          hashVal = HashingVectorColumnProcessors.NULL_EMPTY_HASH;
        }
      }
      dictionaryIdToHash[dictId] = hashVal;
      return hashVal;
    }
  }

  /**
   * Hasher for string column with unknown cardinality. Provides supplier for a long value which works on the current
   * value of the DimensionSelector.
   */
  public static class StringDimWithoutCardinalityHasher implements HashSupplier
  {
    private final DimensionSelector selector;
    private final boolean supportsLookupNameUtf8;

    public StringDimWithoutCardinalityHasher(DimensionSelector selector)
    {
      this.selector = selector;
      this.supportsLookupNameUtf8 = selector.supportsLookupNameUtf8();
    }

    @Override
    public long getHash()
    {
      IndexedInts indexedInts = selector.getRow();
      if (indexedInts.size() == 0) {
        return HashingVectorColumnProcessors.NULL_EMPTY_HASH;
      }
      if (indexedInts.size() > 1) {
        throw new UnsupportedOperationException(
            "Multi Valued Dimensions are not supported as grouping columns in "
            + SamplingGroupByQuery.QUERY_TYPE + " query");
      }

      int dictId = indexedInts.get(0);
      if (supportsLookupNameUtf8) { // fast path since their is no conversion from UTF-8 to UTF-16LE (java string encoding)
        ByteBuffer value = selector.lookupNameUtf8(dictId);
        if (value != null && value.hasRemaining()) {
          byte[] bytes = new byte[value.remaining()];
          value.mark();
          value.get(bytes);
          value.reset();
          return MurmurHash3.hash(bytes, Util.DEFAULT_UPDATE_SEED)[0] >>> 1;
        } else {
          return HashingVectorColumnProcessors.NULL_EMPTY_HASH;
        }
      } else {
        String value = selector.lookupName(dictId);
        if (value != null && !value.isEmpty()) {
          return MurmurHash3.hash(value.getBytes(StandardCharsets.UTF_8), Util.DEFAULT_UPDATE_SEED)[0] >>> 1;
        } else {
          return HashingVectorColumnProcessors.NULL_EMPTY_HASH;
        }
      }
    }
  }

  /**
   * Hasher for string column with unknown cardinality. Provides supplier for a long value which works on the current
   * value of the DimensionSelector. Also, caches the hash using a bounded map where the key of a map entry represents the
   * dictionary id for the column and the value of map entry represents the hash value.
   */
  public static class CachingStringDimWithoutCardinalityHasher implements HashSupplier
  {
    private final DimensionSelector selector;
    private final boolean supportsLookupNameUtf8;
    private final Map<Integer, Long> dictIdToHashCache;

    public CachingStringDimWithoutCardinalityHasher(DimensionSelector selector, int maxCacheSize)
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
    public long getHash()
    {
      IndexedInts indexedInts = selector.getRow();
      if (indexedInts.size() == 0) {
        return HashingVectorColumnProcessors.NULL_EMPTY_HASH;
      }
      if (indexedInts.size() > 1) {
        throw new UnsupportedOperationException(
            "Multi Valued Dimensions are not supported as grouping columns in "
            + SamplingGroupByQuery.QUERY_TYPE + " query");
      }

      int dictId = indexedInts.get(0);
      if (supportsLookupNameUtf8) { // fast path since their is no conversion from UTF-8 to UTF-16LE (java string encoding)
        return dictIdToHashCache.computeIfAbsent(dictId, id -> {
          ByteBuffer value = selector.lookupNameUtf8(id);
          if (value != null && value.hasRemaining()) {
            byte[] bytes = new byte[value.remaining()];
            value.mark();
            value.get(bytes);
            value.reset();
            return MurmurHash3.hash(bytes, Util.DEFAULT_UPDATE_SEED)[0] >>> 1;
          } else {
            return HashingVectorColumnProcessors.NULL_EMPTY_HASH;
          }
        });
      } else {
        return dictIdToHashCache.computeIfAbsent(dictId, id -> {
          String value = selector.lookupName(id);
          if (value != null && !value.isEmpty()) {
            return MurmurHash3.hash(value.getBytes(StandardCharsets.UTF_8), Util.DEFAULT_UPDATE_SEED)[0] >>> 1;
          } else {
            return HashingVectorColumnProcessors.NULL_EMPTY_HASH;
          }
        });
      }
    }
  }
}
