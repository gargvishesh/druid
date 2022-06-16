/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.query.samplinggroupby;

import org.apache.druid.query.dimension.DimensionSpec;
import org.apache.druid.query.filter.Filter;
import org.apache.druid.segment.ColumnInspector;
import org.apache.druid.segment.StorageAdapter;
import org.apache.druid.segment.VirtualColumns;
import org.apache.druid.segment.column.ColumnCapabilities;

import javax.annotation.Nullable;
import java.util.List;

public class SamplingGroupByUtils
{
  public static boolean canVectorize(
      final SamplingGroupByQuery query,
      final StorageAdapter adapter,
      @Nullable final Filter filter
  )
  {
    final ColumnInspector inspector = query.getVirtualColumns().wrapInspector(adapter);

    return adapter.canVectorize(filter, query.getVirtualColumns(), false)
           && canVectorizeDimensions(inspector, query.getDimensions())
           && VirtualColumns.shouldVectorize(query, query.getVirtualColumns(), adapter);
  }

  public static boolean canVectorizeDimensions(
      final ColumnInspector inspector,
      final List<DimensionSpec> dimensions
  )
  {
    return dimensions
        .stream()
        .allMatch(
            dimension -> {
              if (!dimension.canVectorize()) {
                return false;
              }

              if (dimension.mustDecorate()) {
                // group by on multi value dimensions are not currently supported
                // DimensionSpecs that decorate may turn singly-valued columns into multi-valued selectors.
                // To be safe, we must return false here.
                return false;
              }

              // Now check column capabilities.
              final ColumnCapabilities columnCapabilities = inspector.getColumnCapabilities(dimension.getDimension());
              // null here currently means the column does not exist, nil columns can be vectorized
              if (columnCapabilities == null) {
                return true;
              }
              // must be single valued
              return columnCapabilities.hasMultipleValues().isFalse();
            });
  }
}
