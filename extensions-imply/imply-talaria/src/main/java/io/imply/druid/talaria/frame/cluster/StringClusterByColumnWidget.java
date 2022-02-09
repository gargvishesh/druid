/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.talaria.frame.cluster;

import org.apache.druid.query.dimension.DefaultDimensionSpec;
import org.apache.druid.segment.ColumnSelectorFactory;
import org.apache.druid.segment.ColumnValueSelector;

import javax.annotation.Nullable;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.function.Supplier;

public class StringClusterByColumnWidget implements ClusterByColumnWidget<Object, ColumnValueSelector>
{
  private static final Comparator<Object> OBJECT_COMPARATOR =
      new Comparator<Object>()
      {
        @Override
        @SuppressWarnings("unchecked")
        public int compare(final Object o1, final Object o2)
        {
          // TODO(gianm): Check if behavior is, or should be, consistent with StringDimensionHandler.DIMENSION_SELECTOR_COMPARATOR.
          if (!(o1 instanceof List) && !(o2 instanceof List)) {
            // Both are nulls or simple strings.
            return compareStrings((String) o1, (String) o2);
          } else {
            // At least one is a list. Normalize and compare.
            //noinspection CastConflictsWithInstanceof: false alarm
            final List<String> l1 = o1 instanceof List ? (List<String>) o1 : Collections.singletonList((String) o1);

            //noinspection CastConflictsWithInstanceof: false alarm
            final List<String> l2 = o2 instanceof List ? (List<String>) o2 : Collections.singletonList((String) o2);

            for (int i = 0; i < l1.size() && i < l2.size(); i++) {
              final int cmp = compareStrings(l1.get(i), l2.get(i));
              if (cmp != 0) {
                return cmp;
              }
            }

            return Integer.compare(l1.size(), l2.size());
          }
        }
      };

  private final String columnName;
  private final boolean descending;
  private final boolean isArray;

  StringClusterByColumnWidget(String columnName, boolean descending, boolean isArray)
  {
    this.columnName = columnName;
    this.descending = descending;
    this.isArray = isArray;
  }

  @Override
  public ColumnValueSelector makeSelector(ColumnSelectorFactory columnSelectorFactory)
  {
    return isArray ?
           columnSelectorFactory.makeColumnValueSelector(columnName) :
           columnSelectorFactory.makeDimensionSelector(DefaultDimensionSpec.of(columnName));
  }

  @Override
  public Supplier<Object> reader(ColumnValueSelector selector)
  {
    return selector::getObject;
  }

  @Override
  public Comparator<Object> objectComparator()
  {
    return descending ? OBJECT_COMPARATOR.reversed() : OBJECT_COMPARATOR;
  }

  private static int compareStrings(@Nullable final String s1, @Nullable final String s2)
  {
    if (s1 == null) {
      return s2 == null ? 0 : -1;
    } else if (s2 == null) {
      return 1;
    } else {
      return s1.compareTo(s2);
    }
  }
}
