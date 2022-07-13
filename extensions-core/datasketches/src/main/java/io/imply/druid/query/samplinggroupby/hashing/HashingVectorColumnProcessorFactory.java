/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.query.samplinggroupby.hashing;

import com.google.common.base.Preconditions;
import io.imply.druid.query.samplinggroupby.SamplingGroupByQuery;
import org.apache.datasketches.Util;
import org.apache.datasketches.hash.MurmurHash3;
import org.apache.druid.annotations.EverythingIsNonnullByDefault;
import org.apache.druid.segment.DimensionDictionarySelector;
import org.apache.druid.segment.VectorColumnProcessorFactory;
import org.apache.druid.segment.column.ColumnCapabilities;
import org.apache.druid.segment.column.ValueType;
import org.apache.druid.segment.vector.MultiValueDimensionVectorSelector;
import org.apache.druid.segment.vector.SingleValueDimensionVectorSelector;
import org.apache.druid.segment.vector.VectorObjectSelector;
import org.apache.druid.segment.vector.VectorValueSelector;

import java.nio.charset.StandardCharsets;

/**
 * This factory provides a vectorized interface to hash the values of a column of any type. For string columns, it is
 * also tried to cache the hashes to avoid recomputation.
 */
@EverythingIsNonnullByDefault
public class HashingVectorColumnProcessorFactory implements VectorColumnProcessorFactory<HashVectorSupplier>
{
  private static final HashingVectorColumnProcessorFactory INSTANCE = new HashingVectorColumnProcessorFactory();

  private HashingVectorColumnProcessorFactory()
  {
    // Singleton.
  }

  public static HashingVectorColumnProcessorFactory instance()
  {
    return INSTANCE;
  }

  @Override
  public HashVectorSupplier makeSingleValueDimensionProcessor(
      ColumnCapabilities capabilities,
      SingleValueDimensionVectorSelector selector
  )
  {
    Preconditions.checkArgument(
        capabilities.is(ValueType.STRING),
        "dimension processors must be STRING typed"
    );
    int maxEntriesForArrayBasedCaching = 128_000; // leads to a 1MB array
    if (selector.nameLookupPossibleInAdvance()) {
      if (selector.getValueCardinality() == DimensionDictionarySelector.CARDINALITY_UNKNOWN) {
        return new HashingVectorColumnProcessors.StringDimWithoutCardinalityHasher(selector); // don't cache the hash
      } else if (selector.getValueCardinality() <= maxEntriesForArrayBasedCaching) {
        return new HashingVectorColumnProcessors.CachingStringDimWithCardinalityHasher(selector); // cache with an array
      } else {
        return new HashingVectorColumnProcessors.CachingStringDimWithoutCardinalityHasher(
            selector,
            maxEntriesForArrayBasedCaching
        ); // cache with a map
      }
    } else {
      return new HashingVectorColumnProcessors.StringDimWithoutCardinalityHasher(selector); // don't cache the hash
    }
  }

  @Override
  public HashVectorSupplier makeMultiValueDimensionProcessor(
      ColumnCapabilities capabilities,
      MultiValueDimensionVectorSelector selector
  )
  {
    throw new UnsupportedOperationException(
        "Multi Valued Dimensions are not supported as grouping columns in "
        + SamplingGroupByQuery.QUERY_TYPE + " query");
  }

  @Override
  public HashVectorSupplier makeFloatProcessor(
      ColumnCapabilities capabilities,
      VectorValueSelector selector
  )
  {
    // TODO : do we need small cache for value -> hash?
    // using the same hash value for 0 and null. It doesn't affect the correctness of the results.
    // The distribution of the data changes a bit but that's ok for now.
    return (startOffset, endOffset) -> {
      float[] vector = selector.getFloatVector();
      int vSize = selector.getCurrentVectorSize();
      long[] resultHashes = new long[vSize];
      for (int i = startOffset; i < endOffset; i++) {
        resultHashes[i - startOffset] = MurmurHash3.hash(Float.floatToIntBits(vector[i]), Util.DEFAULT_UPDATE_SEED)[0] >>> 1;
      }
      return resultHashes;
    };
  }

  @Override
  public HashVectorSupplier makeDoubleProcessor(
      ColumnCapabilities capabilities,
      VectorValueSelector selector
  )
  {
    return (startOffset, endOffset) -> {
      double[] vector = selector.getDoubleVector();
      int vSize = selector.getCurrentVectorSize();
      long[] resultHashes = new long[vSize];
      for (int i = startOffset; i < endOffset; i++) {
        resultHashes[i - startOffset] = MurmurHash3.hash(Double.doubleToLongBits(vector[i]), Util.DEFAULT_UPDATE_SEED)[0] >>> 1;
      }
      return resultHashes;
    };
  }

  @Override
  public HashVectorSupplier makeLongProcessor(
      ColumnCapabilities capabilities,
      VectorValueSelector selector
  )
  {
    return (startOffset, endOffset) -> {
      long[] vector = selector.getLongVector();
      int vSize = selector.getCurrentVectorSize();
      long[] resultHashes = new long[vSize];
      for (int i = startOffset; i < endOffset; i++) {
        resultHashes[i - startOffset] = MurmurHash3.hash(vector[i], Util.DEFAULT_UPDATE_SEED)[0] >>> 1;
      }
      return resultHashes;
    };
  }

  @Override
  public HashVectorSupplier makeObjectProcessor(
      ColumnCapabilities capabilities,
      VectorObjectSelector selector
  )
  {
    if (!capabilities.is(ValueType.STRING)) {
      throw new UnsupportedOperationException("Grouping on dimensions with complex columns or expressions which are " +
                                              "not STRING type is unsupported");
    }
    // TODO : adding caching
    return (startOffset, endOffset) -> {
      Object[] vector = selector.getObjectVector();
      int vSize = selector.getCurrentVectorSize();
      long[] resultHashes = new long[vSize];
      for (int i = startOffset; i < endOffset; i++) {
        if (vector[i] == null) {
          resultHashes[i] = HashingVectorColumnProcessors.NULL_EMPTY_HASH;
        } else {
          resultHashes[i - startOffset] = MurmurHash3.hash(
              vector[i].toString().getBytes(StandardCharsets.UTF_8), Util.DEFAULT_UPDATE_SEED
          )[0] >>> 1;
        }
      }
      return resultHashes;
    };
  }
}
