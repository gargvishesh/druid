/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.talaria.frame.cluster;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.segment.ColumnSelectorFactory;
import org.apache.druid.segment.column.ColumnCapabilities;
import org.apache.druid.segment.column.RowSignature;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.function.Supplier;

/**
 * Represents a partitioning key, ordering key, or both.
 */
public class ClusterBy
{
  private final List<ClusterByColumn> columns;
  private final int bucketByCount;

  @JsonCreator
  public ClusterBy(
      @JsonProperty("columns") List<ClusterByColumn> columns,
      @JsonProperty("bucketByCount") int bucketByCount
  )
  {
    this.columns = Preconditions.checkNotNull(columns, "columns");
    this.bucketByCount = bucketByCount;

    if (bucketByCount < 0 || bucketByCount > columns.size()) {
      throw new IAE("Invalid bucketByCount [%d]", bucketByCount);
    }
  }

  /**
   * Create an empty key.
   */
  public static ClusterBy none()
  {
    return new ClusterBy(Collections.emptyList(), 0);
  }

  /**
   * The columns that comprise this key, in order.
   */
  @JsonProperty
  public List<ClusterByColumn> getColumns()
  {
    return columns;
  }

  /**
   * How many fields from {@link #getColumns()} comprise the "bucket key". Bucketing is like strict partitioning: all
   * rows in a given partition will have the exact same bucket key. It is most commonly used to implement
   * segment granularity during ingestion.
   *
   * Will always be less than, or equal to, the size of {@link #getColumns()}.
   *
   * Not relevant when a ClusterBy instance is used as an ordering key.
   *
   * TODO(gianm): Rename to getPartitionGroupKeySize
   */
  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_DEFAULT)
  public int getBucketByCount()
  {
    return bucketByCount;
  }

  /**
   * Supplier that reads keys for this instance out of a given {@link ColumnSelectorFactory} using the given signature.
   */
  public Supplier<ClusterByKey> keyReader(
      final ColumnSelectorFactory columnSelectorFactory,
      final RowSignature signature
  )
  {
    final List<Supplier<Object>> partReaders = new ArrayList<>();

    for (final ClusterByColumn part : columns) {
      final ColumnCapabilities capabilities = signature.getColumnCapabilities(part.columnName());
      if (capabilities == null) {
        throw new ISE("Column [%s] not found, cannot use it in clusterBy", part.columnName());
      }

      //noinspection rawtypes
      final ClusterByColumnWidget widget = ClusterByColumnWidgets.create(part, capabilities.getType());
      //noinspection unchecked
      partReaders.add(widget.reader(widget.makeSelector(columnSelectorFactory)));
    }

    return () -> {
      final Object[] keyArray = new Object[partReaders.size()];

      for (int i = 0; i < keyArray.length; i++) {
        keyArray[i] = partReaders.get(i).get();
      }

      return ClusterByKey.of(keyArray);
    };
  }

  /**
   * Comparator that compares keys for this instance using the given signature.
   */
  public Comparator<ClusterByKey> keyComparator(final RowSignature signature)
  {
    return new ClusterByKeyComparator(this, Preconditions.checkNotNull(signature, "signature"));
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    ClusterBy clusterBy = (ClusterBy) o;
    return bucketByCount == clusterBy.bucketByCount && Objects.equals(columns, clusterBy.columns);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(columns, bucketByCount);
  }

  @Override
  public String toString()
  {
    return "ClusterBy{" +
           "columns=" + columns +
           ", bucketByCount=" + bucketByCount +
           '}';
  }
}
