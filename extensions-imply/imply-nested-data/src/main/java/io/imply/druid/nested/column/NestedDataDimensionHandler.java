/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.nested.column;

import org.apache.druid.data.input.impl.DimensionSchema;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.query.dimension.DefaultDimensionSpec;
import org.apache.druid.query.dimension.DimensionSpec;
import org.apache.druid.segment.ColumnValueSelector;
import org.apache.druid.segment.DimensionHandler;
import org.apache.druid.segment.DimensionIndexer;
import org.apache.druid.segment.DimensionMergerV9;
import org.apache.druid.segment.IndexSpec;
import org.apache.druid.segment.ProgressIndicator;
import org.apache.druid.segment.column.ColumnCapabilities;
import org.apache.druid.segment.selector.settable.SettableColumnValueSelector;
import org.apache.druid.segment.selector.settable.SettableObjectColumnValueSelector;
import org.apache.druid.segment.writeout.SegmentWriteOutMedium;

import java.util.Comparator;

public class NestedDataDimensionHandler implements DimensionHandler<StructuredData, StructuredData, StructuredData>
{
  private static Comparator<ColumnValueSelector> COMPARATOR = (s1, s2) ->
      StructuredData.COMPARATOR.compare((StructuredData) s1.getObject(), (StructuredData) s2.getObject());

  private final String name;

  public NestedDataDimensionHandler(String name)
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
    return new DefaultDimensionSpec(name, name, NestedDataComplexTypeSerde.TYPE);
  }

  @Override
  public DimensionSchema getDimensionSchema(ColumnCapabilities capabilities)
  {
    return new NestedDataDimensionSchema(name);
  }

  @Override
  public DimensionIndexer<StructuredData, StructuredData, StructuredData> makeIndexer()
  {
    return new NestedDataColumnIndexer();
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
    return new NestedDataColumnMerger(name, indexSpec, segmentWriteOutMedium, capabilities, progress, closer);
  }

  @Override
  public int getLengthOfEncodedKeyComponent(StructuredData dimVals)
  {
    // this is called in one place, OnheapIncrementalIndex, where returning 0 here means the value is null
    // so the actual value we return here doesn't matter. we should consider refactoring this to a boolean
    return 1;
  }

  @Override
  public Comparator<ColumnValueSelector> getEncodedValueSelectorComparator()
  {
    return COMPARATOR;
  }

  @Override
  public SettableColumnValueSelector makeNewSettableEncodedValueSelector()
  {
    return new SettableObjectColumnValueSelector();
  }
}
