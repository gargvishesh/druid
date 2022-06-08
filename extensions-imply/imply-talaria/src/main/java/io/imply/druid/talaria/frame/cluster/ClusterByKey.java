/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.talaria.frame.cluster;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import com.google.common.hash.Hashing;

import java.util.Arrays;

/**
 * Represents a specific cluster key associated with a {@link ClusterBy} instance.
 *
 * Instances of this class wrap a byte array in row-based frame format.
 */
public class ClusterByKey
{
  private static final ClusterByKey EMPTY_KEY = new ClusterByKey(new byte[0]);

  private final byte[] key;

  // Cached hashcode. Computed on demand, not in the constructor, to avoid unnecessary computation.
  private volatile long hashCode;
  private volatile boolean hashCodeComputed;

  private ClusterByKey(byte[] key)
  {
    this.key = key;
  }

  /**
   * Create a key from a byte array. The array will be owned by the resulting key object.
   */
  @JsonCreator
  public static ClusterByKey wrap(final byte[] row)
  {
    if (row.length == 0) {
      return EMPTY_KEY;
    } else {
      return new ClusterByKey(row);
    }
  }

  public static ClusterByKey empty()
  {
    return EMPTY_KEY;
  }

  /**
   * Get the backing array for this key (not a copy).
   */
  @JsonValue
  public byte[] array()
  {
    return key;
  }

  public long longHashCode()
  {
    // May compute hashCode multiple times if called from different threads, but that's fine. (And unlikely, given
    // how we use these objects.)
    if (!hashCodeComputed) {
      // Use murmur3_128 for dispersion properties needed by DistinctKeyCollector#isKeySelected.
      hashCode = Hashing.murmur3_128().hashBytes(key).asLong();
      hashCodeComputed = true;
    }

    return hashCode;
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    ClusterByKey that = (ClusterByKey) o;
    return Arrays.equals(key, that.key);
  }

  @Override
  public int hashCode()
  {
    // Truncation is OK with murmur3_128.
    return (int) longHashCode();
  }

  @Override
  public String toString()
  {
    return Arrays.toString(key);
  }
}
