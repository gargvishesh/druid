/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.talaria.frame.cluster;

import org.apache.druid.java.util.common.guava.Comparators;
import org.apache.druid.segment.BaseLongColumnValueSelector;
import org.apache.druid.segment.ColumnSelectorFactory;

import java.util.Comparator;
import java.util.function.Supplier;

public class LongClusterByColumnWidget implements ClusterByColumnWidget<Long, BaseLongColumnValueSelector>
{
  private final String columnName;
  private final boolean descending;

  LongClusterByColumnWidget(String columnName, boolean descending)
  {
    this.columnName = columnName;
    this.descending = descending;
  }

  @Override
  public BaseLongColumnValueSelector makeSelector(ColumnSelectorFactory columnSelectorFactory)
  {
    return columnSelectorFactory.makeColumnValueSelector(columnName);
  }

  @Override
  public Supplier<Long> reader(BaseLongColumnValueSelector selector)
  {
    return () -> selector.isNull() ? null : selector.getLong();
  }

  @Override
  public Comparator<Long> objectComparator()
  {
    final Comparator<Long> baseComparator = Comparators.naturalNullsFirst();
    return descending ? baseComparator.reversed() : baseComparator;
  }
}
