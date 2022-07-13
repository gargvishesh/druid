/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.talaria.querykit;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.imply.druid.talaria.frame.cluster.ClusterBy;
import io.imply.druid.talaria.frame.cluster.ClusterByColumn;
import org.apache.calcite.sql.dialect.CalciteSqlDialect;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.granularity.Granularity;
import org.apache.druid.java.util.common.granularity.PeriodGranularity;
import org.apache.druid.math.expr.ExprMacroTable;
import org.apache.druid.query.Query;
import org.apache.druid.query.expression.TimestampFloorExprMacro;
import org.apache.druid.segment.VirtualColumn;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.segment.virtual.ExpressionVirtualColumn;
import org.apache.druid.sql.calcite.parser.DruidSqlInsert;
import org.apache.druid.sql.calcite.planner.Calcites;
import org.joda.time.DateTimeZone;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Utility methods for QueryKit.
 */
public class QueryKitUtils
{
  // TODO(gianm): use safer name for __boost column
  // TODO(gianm): add partition boosting to groupBy too (only when there is order by!)
  public static final String PARTITION_BOOST_COLUMN = "__boost";

  // TODO(gianm): use safer name for __bucket column
  public static final String SEGMENT_GRANULARITY_COLUMN = "__bucket";

  // TODO(gianm): hack alert: this is redundant to the ColumnMappings, but is here because QueryKit doesn't get those
  public static final String CTX_TIME_COLUMN_NAME = "msqTimeColumn";

  private static final ObjectMapper OBJECT_MAPPER = new DefaultObjectMapper();

  public static Granularity getSegmentGranularityFromContext(@Nullable final Map<String, Object> context)
  {
    final Object o = context == null ? null : context.get(DruidSqlInsert.SQL_INSERT_SEGMENT_GRANULARITY);

    if (o instanceof String) {
      try {
        return OBJECT_MAPPER.readValue((String) o, Granularity.class);
      }
      catch (JsonProcessingException e) {
        throw new ISE("Invalid segment granularity [%s]", o);
      }
    } else if (o == null) {
      return Granularities.ALL;
    } else {
      throw new ISE("Invalid segment granularity [%s]", o);
    }
  }

  /**
   * Adds bucketing by {@link #SEGMENT_GRANULARITY_COLUMN} to a {@link ClusterBy} if needed.
   */
  public static ClusterBy clusterByWithSegmentGranularity(
      final ClusterBy clusterBy,
      final Granularity segmentGranularity
  )
  {
    if (Granularities.ALL.equals(segmentGranularity)) {
      return clusterBy;
    } else {
      final List<ClusterByColumn> newColumns = new ArrayList<>(clusterBy.getColumns().size() + 1);
      newColumns.add(new ClusterByColumn(QueryKitUtils.SEGMENT_GRANULARITY_COLUMN, false));
      newColumns.addAll(clusterBy.getColumns());
      return new ClusterBy(newColumns, 1);
    }
  }

  /**
   * Adds {@link #SEGMENT_GRANULARITY_COLUMN} to a {@link RowSignature} if needed.
   */
  public static RowSignature signatureWithSegmentGranularity(
      final RowSignature signature,
      final Granularity segmentGranularity
  )
  {
    if (Granularities.ALL.equals(segmentGranularity)) {
      return signature;
    } else {
      if (signature.contains(QueryKitUtils.SEGMENT_GRANULARITY_COLUMN)) {
        throw new ISE("Cannot use reserved column [%s]", QueryKitUtils.SEGMENT_GRANULARITY_COLUMN);
      }

      return RowSignature.builder()
                         .addAll(signature)
                         .add(QueryKitUtils.SEGMENT_GRANULARITY_COLUMN, ColumnType.LONG)
                         .build();
    }
  }

  /**
   * Returns a copy of "signature" with columns rearranged so the provided clusterByColumns appear as a prefix.
   * Throws an error if any of the clusterByColumns are not present in the input signature, or if any of their
   * types are unknown.
   */
  public static RowSignature sortableSignature(
      final RowSignature signature,
      final List<ClusterByColumn> clusterByColumns
  )
  {
    final RowSignature.Builder builder = RowSignature.builder();

    for (final ClusterByColumn columnName : clusterByColumns) {
      final Optional<ColumnType> columnType = signature.getColumnType(columnName.columnName());
      if (!columnType.isPresent()) {
        throw new IAE("Column [%s] not present in signature", columnName);
      }

      builder.add(columnName.columnName(), columnType.get());
    }

    final Set<String> clusterByColumnNames =
        clusterByColumns.stream().map(ClusterByColumn::columnName).collect(Collectors.toSet());

    for (int i = 0; i < signature.size(); i++) {
      final String columnName = signature.getColumnName(i);
      if (!clusterByColumnNames.contains(columnName)) {
        builder.add(columnName, signature.getColumnType(i).orElse(null));
      }
    }

    return builder.build();
  }

  /**
   * Returns a virtual column named {@link QueryKitUtils#SEGMENT_GRANULARITY_COLUMN} that computes a segment
   * granularity based on a particular time column. Returns null if no virtual column is needed because the
   * granularity is {@link Granularities#ALL}. Throws an exception if the provided granularity is not supported.
   *
   * @throws IllegalArgumentException if the provided granularity is not supported
   */
  @Nullable
  public static VirtualColumn makeSegmentGranularityVirtualColumn(final Query<?> query)
  {
    final Granularity segmentGranularity = QueryKitUtils.getSegmentGranularityFromContext(query.getContext());
    final String timeColumnName = query.getContextValue(QueryKitUtils.CTX_TIME_COLUMN_NAME);

    if (timeColumnName == null || Granularities.ALL.equals(segmentGranularity)) {
      return null;
    }

    if (!(segmentGranularity instanceof PeriodGranularity)) {
      throw new IAE("Granularity [%s] is not supported", segmentGranularity);
    }

    final PeriodGranularity periodSegmentGranularity = (PeriodGranularity) segmentGranularity;

    if (periodSegmentGranularity.getOrigin() != null
        || !periodSegmentGranularity.getTimeZone().equals(DateTimeZone.UTC)) {
      throw new IAE("Granularity [%s] is not supported", segmentGranularity);
    }

    return new ExpressionVirtualColumn(
        QueryKitUtils.SEGMENT_GRANULARITY_COLUMN,
        StringUtils.format(
            "timestamp_floor(%s, %s)",
            CalciteSqlDialect.DEFAULT.quoteIdentifier(timeColumnName),
            Calcites.escapeStringLiteral((periodSegmentGranularity).getPeriod().toString())
        ),
        ColumnType.LONG,
        new ExprMacroTable(Collections.singletonList(new TimestampFloorExprMacro()))
    );
  }
}
