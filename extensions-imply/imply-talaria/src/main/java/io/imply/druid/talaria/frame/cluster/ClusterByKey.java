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
import org.apache.druid.java.util.common.IAE;

import java.util.Arrays;

/**
 * Represents a specific cluster key associated with a {@link ClusterBy} instance.
 */
public class ClusterByKey
{
  private static final ClusterByKey EMPTY_KEY = new ClusterByKey(new Object[0]);

  private final Object[] key;

  private int hashCode;
  private boolean hashCodeComputed;

  private ClusterByKey(Object[] key)
  {
    this.key = key;
  }

  /**
   * Create a key from an array of objects.
   */
  @JsonCreator
  public static ClusterByKey of(final Object... row)
  {
    if (row.length == 0) {
      return EMPTY_KEY;
    } else {
      return new ClusterByKey(row);
    }
  }

  public Object get(final int i)
  {
    return key[i];
  }

  public ClusterByKey trim(final int newLength)
  {
    if (newLength == 0) {
      return EMPTY_KEY;
    } else if (key.length < newLength) {
      throw new IAE("Cannot trim");
    } else if (key.length == newLength) {
      return this;
    } else {
      final Object[] trimmedArray = new Object[newLength];
      System.arraycopy(key, 0, trimmedArray, 0, newLength);
      return of(trimmedArray);
    }
  }

  /**
   * Get the backing array for this key (not a copy).
   */
  @JsonValue
  Object[] getArray()
  {
    return key;
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
    // Check is not thread-safe, but that's fine. Even if used by multiple threads, it's ok to write these primitive
    // fields more than once.
    if (!hashCodeComputed) {
      // Use murmur3_32 for dispersion properties needed by DistinctKeyCollector#isKeySelected.
      hashCode = Hashing.murmur3_32().hashInt(Arrays.hashCode(key)).asInt();
      hashCodeComputed = true;
    }

    return hashCode;
  }

  @Override
  public String toString()
  {
    return Arrays.toString(key);
  }
}
