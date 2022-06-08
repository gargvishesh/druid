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
import org.apache.druid.java.util.common.guava.Comparators;
import org.apache.druid.segment.ColumnInspector;
import org.apache.druid.segment.column.ColumnCapabilities;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;

import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;

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
   */
  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_DEFAULT)
  public int getBucketByCount()
  {
    return bucketByCount;
  }

  /**
   * Create a reader for keys for this instance.
   *
   * The provided {@link ColumnInspector} is used to determine the types of fields in the keys. The provided signature
   * does not have to exactly match the sortColumns: it merely has to contain them all.
   */
  public ClusterByKeyReader keyReader(final ColumnInspector inspector)
  {
    final RowSignature.Builder newSignature = RowSignature.builder();

    for (final ClusterByColumn sortColumn : columns) {
      final String columnName = sortColumn.columnName();
      final ColumnCapabilities capabilities = inspector.getColumnCapabilities(columnName);
      final ColumnType columnType =
          Preconditions.checkNotNull(capabilities, "Type for column [%s]", columnName).toColumnType();

      newSignature.add(columnName, columnType);
    }

    return ClusterByKeyReader.create(newSignature.build());
  }

  /**
   * Comparator that compares keys for this instance using the given signature.
   */
  public Comparator<ClusterByKey> keyComparator()
  {
    return ClusterByKeyComparator.create(columns);
  }

  /**
   * Comparator that compares bucket keys for this instance. Bucket keys are retrieved by calling
   * {@link ClusterByKeyReader#trim(ClusterByKey, int)} with {@link #getBucketByCount()}.
   */
  public Comparator<ClusterByKey> bucketComparator()
  {
    if (bucketByCount == 0) {
      return Comparators.alwaysEqual();
    } else {
      return ClusterByKeyComparator.create(columns.subList(0, bucketByCount));
    }
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
