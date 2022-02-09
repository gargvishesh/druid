/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.talaria.frame.cluster;

import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

public class ClusterByKeyComparator implements Comparator<ClusterByKey>
{
  private final List<Comparator<Object>> partComparators;

  /**
   * Use {@link ClusterBy#keyComparator(RowSignature)}.
   */
  ClusterByKeyComparator(final ClusterBy clusterBy, final RowSignature signature)
  {
    this.partComparators = new ArrayList<>();

    for (final ClusterByColumn part : clusterBy.getColumns()) {
      final ColumnType type = signature.getColumnType(part.columnName()).orElseThrow(
          () -> new ISE("Column [%s] type unknown, cannot create comparator", part.columnName())
      );

      //noinspection rawtypes
      final ClusterByColumnWidget widget = ClusterByColumnWidgets.create(part, type);
      //noinspection unchecked
      this.partComparators.add(widget.objectComparator());
    }
  }

  @Override
  public int compare(ClusterByKey key1, ClusterByKey key2)
  {
    final Object[] array1 = key1.getArray();
    final Object[] array2 = key2.getArray();

    if (array1.length != array2.length) {
      throw new IAE("Cannot compare keys of different lengths");
    }

    for (int i = 0; i < array1.length; i++) {
      final int cmp = partComparators.get(i).compare(array1[i], array2[i]);
      if (cmp != 0) {
        return cmp;
      }
    }

    return 0;
  }
}
