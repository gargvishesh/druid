/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.talaria;

import com.google.common.collect.Iterables;
import org.apache.druid.segment.QueryableIndex;
import org.apache.druid.segment.QueryableIndexStorageAdapter;
import org.apache.druid.segment.column.ColumnCapabilities;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;

import java.util.Optional;

public class TestArrayStorageAdapter extends QueryableIndexStorageAdapter
{
  public TestArrayStorageAdapter(QueryableIndex index)
  {
    super(index);
  }

  @Override
  public RowSignature getRowSignature()
  {
    final RowSignature.Builder builder = RowSignature.builder();
    builder.addTimeColumn();

    for (final String column : Iterables.concat(getAvailableDimensions(), getAvailableMetrics())) {
      Optional<ColumnCapabilities> columnCapabilities = Optional.ofNullable(getColumnCapabilities(column));
      ColumnType columnType = columnCapabilities.isPresent() ? columnCapabilities.get().toColumnType() : null;
      //change MV columns to Array<String>
      if (columnCapabilities.isPresent() && columnCapabilities.get().hasMultipleValues().isMaybeTrue()) {
        columnType = ColumnType.STRING_ARRAY;
      }
      builder.add(column, columnType);
    }

    return builder.build();
  }
}
