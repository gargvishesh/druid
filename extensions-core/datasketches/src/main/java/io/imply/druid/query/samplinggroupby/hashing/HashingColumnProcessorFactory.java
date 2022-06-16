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
import org.apache.druid.annotations.EverythingIsNonnullByDefault;
import org.apache.druid.segment.BaseDoubleColumnValueSelector;
import org.apache.druid.segment.BaseFloatColumnValueSelector;
import org.apache.druid.segment.BaseLongColumnValueSelector;
import org.apache.druid.segment.BaseObjectColumnValueSelector;
import org.apache.druid.segment.ColumnProcessorFactory;
import org.apache.druid.segment.DimensionDictionarySelector;
import org.apache.druid.segment.DimensionSelector;
import org.apache.druid.segment.column.ColumnType;

/**
 * This factory provides an interface to hash the values of a column of any type. For string columns, it is also tried
 * to cache the hashes to avoid recomputation.
 */
@EverythingIsNonnullByDefault
public class HashingColumnProcessorFactory implements ColumnProcessorFactory<HashSupplier>
{
  private static final HashingColumnProcessorFactory INSTANCE = new HashingColumnProcessorFactory();

  private HashingColumnProcessorFactory()
  {
    // Singleton.
  }

  public static HashingColumnProcessorFactory instance()
  {
    return INSTANCE;
  }

  @Override
  public ColumnType defaultType()
  {
    return ColumnType.STRING; // easy choice, but is it wrong?
  }

  @Override
  public HashSupplier makeDimensionProcessor(DimensionSelector selector, boolean multiValue)
  {
    if (!selector.nameLookupPossibleInAdvance()) {
      return new HashingColumnProcessors.StringDimWithoutCardinalityHasher(selector); // don't cache the hash
    }

    int maxEntriesForArrayBasedCaching = 128_000; // leads to a 1MB array
    if (selector.getValueCardinality() == DimensionDictionarySelector.CARDINALITY_UNKNOWN) {
      return new HashingColumnProcessors.StringDimWithoutCardinalityHasher(selector); // don't cache the hash
    } else if (selector.getValueCardinality() <= maxEntriesForArrayBasedCaching) {
      return new HashingColumnProcessors.CachingStringDimWithCardinalityHasher(selector); // cache with an array
    } else {
      return new HashingColumnProcessors.CachingStringDimWithoutCardinalityHasher(
          selector,
          maxEntriesForArrayBasedCaching
      ); // cache with a map
    }
  }

  @Override
  public HashSupplier makeFloatProcessor(BaseFloatColumnValueSelector selector)
  {
    return () -> MurmurHash3.hash(Float.floatToIntBits(selector.getFloat()), Util.DEFAULT_UPDATE_SEED)[0] >>> 1;
  }

  @Override
  public HashSupplier makeDoubleProcessor(BaseDoubleColumnValueSelector selector)
  {
    return () -> MurmurHash3.hash(Double.doubleToLongBits(selector.getDouble()), Util.DEFAULT_UPDATE_SEED)[0] >>> 1;
  }

  @Override
  public HashSupplier makeLongProcessor(BaseLongColumnValueSelector selector)
  {
    return () -> MurmurHash3.hash(selector.getLong(), Util.DEFAULT_UPDATE_SEED)[0] >>> 1;
  }

  @Override
  public HashSupplier makeComplexProcessor(BaseObjectColumnValueSelector<?> selector)
  {
    return () -> {
      if (selector.getObject() == null) {
        return HashingVectorColumnProcessors.NULL_EMPTY_HASH;
      }
      return MurmurHash3.hash(selector.getObject().hashCode(), Util.DEFAULT_UPDATE_SEED)[0] >>> 1;
    };
  }
}
