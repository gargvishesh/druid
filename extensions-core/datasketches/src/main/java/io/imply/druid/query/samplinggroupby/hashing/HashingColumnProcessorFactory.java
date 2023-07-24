/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.query.samplinggroupby.hashing;

import org.apache.datasketches.hash.MurmurHash3;
import org.apache.datasketches.thetacommon.ThetaUtil;
import org.apache.druid.annotations.EverythingIsNonnullByDefault;
import org.apache.druid.segment.BaseDoubleColumnValueSelector;
import org.apache.druid.segment.BaseFloatColumnValueSelector;
import org.apache.druid.segment.BaseLongColumnValueSelector;
import org.apache.druid.segment.BaseObjectColumnValueSelector;
import org.apache.druid.segment.ColumnProcessorFactory;
import org.apache.druid.segment.DimensionDictionarySelector;
import org.apache.druid.segment.DimensionSelector;
import org.apache.druid.segment.column.ColumnCapabilities;
import org.apache.druid.segment.column.ColumnType;

import javax.annotation.Nullable;

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
    return () -> MurmurHash3.hash(Float.floatToIntBits(selector.getFloat()), ThetaUtil.DEFAULT_UPDATE_SEED)[0] >>> 1;
  }

  @Override
  public HashSupplier makeDoubleProcessor(BaseDoubleColumnValueSelector selector)
  {
    return () -> MurmurHash3.hash(Double.doubleToLongBits(selector.getDouble()), ThetaUtil.DEFAULT_UPDATE_SEED)[0] >>> 1;
  }

  @Override
  public HashSupplier makeLongProcessor(BaseLongColumnValueSelector selector)
  {
    return () -> MurmurHash3.hash(selector.getLong(), ThetaUtil.DEFAULT_UPDATE_SEED)[0] >>> 1;
  }

  @Override
  public HashSupplier makeArrayProcessor(BaseObjectColumnValueSelector<?> selector, @Nullable ColumnCapabilities columnCapabilities)
  {
    throw new UnsupportedOperationException("Grouping on ARRAY columns or expressions which are " +
        "not STRING type is unsupported");
  }

  @Override
  public HashSupplier makeComplexProcessor(BaseObjectColumnValueSelector<?> selector)
  {
    throw new UnsupportedOperationException("Grouping on dimensions with complex columns or expressions which are " +
                                              "not STRING type is unsupported");
  }
}
