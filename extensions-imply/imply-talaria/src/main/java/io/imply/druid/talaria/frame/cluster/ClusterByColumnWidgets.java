/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.talaria.frame.cluster;

import com.google.common.base.Preconditions;
import org.apache.druid.java.util.common.UOE;
import org.apache.druid.segment.column.ColumnType;

public class ClusterByColumnWidgets
{
  @SuppressWarnings("rawtypes")
  public static ClusterByColumnWidget create(final ClusterByColumn part, final ColumnType type)
  {
    Preconditions.checkNotNull(type, "type must be nonnull");

    switch (type.getType()) {
      case STRING:
        return new StringClusterByColumnWidget(part.columnName(), part.descending(), false);
      case ARRAY:
        switch (type.getElementType().getType()) {
          case STRING:
            return new StringClusterByColumnWidget(part.columnName(), part.descending(), true);
          default:
            throw new UOE("Cannot cluster by type [%s]", type);
        }
      case LONG:
        return new LongClusterByColumnWidget(part.columnName(), part.descending());
      case FLOAT:
        return new FloatClusterByColumnWidget(part.columnName(), part.descending());
      case DOUBLE:
        return new DoubleClusterByColumnWidget(part.columnName(), part.descending());
      default:
        throw new UOE("Cannot cluster by type [%s]", type);
    }
  }
}
