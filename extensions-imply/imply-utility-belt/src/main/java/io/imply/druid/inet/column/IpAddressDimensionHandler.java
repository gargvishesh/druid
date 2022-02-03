/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.inet.column;

import io.imply.druid.inet.IpAddressModule;
import org.apache.druid.data.input.impl.DimensionSchema;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.query.dimension.DefaultDimensionSpec;
import org.apache.druid.query.dimension.DimensionSpec;
import org.apache.druid.segment.ColumnValueSelector;
import org.apache.druid.segment.DimensionHandler;
import org.apache.druid.segment.DimensionIndexer;
import org.apache.druid.segment.DimensionMergerV9;
import org.apache.druid.segment.DimensionSelector;
import org.apache.druid.segment.IndexSpec;
import org.apache.druid.segment.NilColumnValueSelector;
import org.apache.druid.segment.ProgressIndicator;
import org.apache.druid.segment.column.ColumnCapabilities;
import org.apache.druid.segment.data.IndexedInts;
import org.apache.druid.segment.selector.settable.SettableColumnValueSelector;
import org.apache.druid.segment.selector.settable.SettableDimensionValueSelector;
import org.apache.druid.segment.writeout.SegmentWriteOutMedium;

import java.util.Comparator;

public class IpAddressDimensionHandler implements DimensionHandler<Integer, Integer, IpAddressBlob>
{
  static final Comparator<ColumnValueSelector> DIMENSION_COMPARATOR = (s1, s2) -> {
    int row1 = getRow(s1);
    int row2 = getRow(s2);

    return Integer.compare(row1, row2);
  };

  private final String name;

  public IpAddressDimensionHandler(String name)
  {
    this.name = name;
  }

  @Override
  public String getDimensionName()
  {
    return name;
  }

  @Override
  public DimensionSpec getDimensionSpec()
  {
    return new DefaultDimensionSpec(name, name, IpAddressModule.TYPE);
  }

  @Override
  public DimensionSchema getDimensionSchema(ColumnCapabilities capabilities)
  {
    return new IpAddressDimensionSchema(name, true);
  }

  @Override
  public DimensionIndexer<Integer, Integer, IpAddressBlob> makeIndexer(boolean useMaxMemoryEstimates)
  {
    return new IpAddressDictionaryEncodedColumnIndexer(true, useMaxMemoryEstimates);
  }

  @Override
  public DimensionMergerV9 makeMerger(
      IndexSpec indexSpec,
      SegmentWriteOutMedium segmentWriteOutMedium,
      ColumnCapabilities capabilities,
      ProgressIndicator progress,
      Closer closer
  )
  {
    return new IpAddressDictionaryEncodedColumnMerger(
        name,
        indexSpec,
        segmentWriteOutMedium,
        capabilities,
        progress,
        closer
    );
  }

  @Override
  public int getLengthOfEncodedKeyComponent(Integer dimVals)
  {
    return 1;
  }

  @Override
  public Comparator<ColumnValueSelector> getEncodedValueSelectorComparator()
  {
    return DIMENSION_COMPARATOR;
  }

  @Override
  public SettableColumnValueSelector makeNewSettableEncodedValueSelector()
  {
    // this has to be a dim selector because merger uses this method and we want to merge on dictionaryId
    return new SettableDimensionValueSelector();
  }

  private static int getRow(ColumnValueSelector s)
  {
    if (s instanceof DimensionSelector) {
      IndexedInts ints = ((DimensionSelector) s).getRow();
      return ints.get(0);
    } else if (s instanceof NilColumnValueSelector) {
      return 0;
    } else {
      throw new ISE(
          "ColumnValueSelector[%s], only DimensionSelector or NilColumnValueSelector is supported",
          s.getClass()
      );
    }
  }
}
