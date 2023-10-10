/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.query.union;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Ordering;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.granularity.Granularity;
import org.apache.druid.query.BaseQuery;
import org.apache.druid.query.DataSource;
import org.apache.druid.query.Query;
import org.apache.druid.query.QueryContexts;
import org.apache.druid.query.QueryRunner;
import org.apache.druid.query.QuerySegmentWalker;
import org.apache.druid.query.filter.DimFilter;
import org.apache.druid.query.spec.QuerySegmentSpec;
import org.apache.druid.segment.VirtualColumns;
import org.joda.time.DateTimeZone;
import org.joda.time.Duration;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class SingleQueryUnionQuery<T> implements Query<T>
{
  private final List<UnionSubQueryOverrides> sources;
  private final BaseQuery<T> subQuery;
  private final Map<String, Object> context;

  public SingleQueryUnionQuery(
      @JsonProperty("sources") List<UnionSubQueryOverrides> sources,
      @JsonProperty("query") Query<T> subQuery,
      @JsonProperty("context") Map<String, Object> context
  )
  {
    this.sources = sources;
    this.context = context;

    if (subQuery instanceof BaseQuery) {
      this.subQuery = (BaseQuery<T>) subQuery;
    } else {
      throw new IAE("singleQueryUnion requires a query that implements BaseQuery, got [%s]", subQuery);
    }
  }

  @JsonProperty("sources")
  public List<UnionSubQueryOverrides> getSources()
  {
    return sources;
  }

  @JsonProperty("query")
  public Query<T> getSubQuery()
  {
    return subQuery;
  }

  @JsonProperty("context")
  @Override
  public Map<String, Object> getContext()
  {
    return context;
  }


  @Override
  public DataSource getDataSource()
  {
    return subQuery.getDataSource();
  }

  @Override
  public boolean hasFilters()
  {
    return subQuery.hasFilters();
  }

  @Override
  public DimFilter getFilter()
  {
    return subQuery.getFilter();
  }

  @Override
  public String getType()
  {
    return "singleQueryUnion";
  }

  @Override
  public QueryRunner<T> getRunner(QuerySegmentWalker walker)
  {
    QuerySegmentSpec dataSource = subQuery.getDataSource()
                                          .getAnalysis()
                                          .getBaseQuerySegmentSpec()
                                          .orElseGet(subQuery::getQuerySegmentSpec);
    return dataSource.lookup(this, walker);
  }

  @Override
  public List<Interval> getIntervals()
  {
    return subQuery.getIntervals();
  }

  @Override
  public Duration getDuration()
  {
    return subQuery.getDuration();
  }

  @Override
  public Granularity getGranularity()
  {
    return subQuery.getGranularity();
  }

  @Override
  public DateTimeZone getTimezone()
  {
    return subQuery.getTimezone();
  }

  @Override
  public boolean isDescending()
  {
    return subQuery.isDescending();
  }

  @Override
  public Ordering<T> getResultOrdering()
  {
    return subQuery.getResultOrdering();
  }

  @Override
  public Query<T> withOverriddenContext(Map<String, Object> contextOverride)
  {
    return new SingleQueryUnionQuery(
        sources,
        subQuery,
        QueryContexts.override(context, contextOverride)
    );
  }

  @Override
  public Query<T> withQuerySegmentSpec(QuerySegmentSpec spec)
  {
    // This should never be called on the UnionQuery.  Instead, it might be called on the subqueries that are
    // fired from the ToolChest merge operator, but that should call the method on the sub-query's class, not
    // UnionQuery.
    throw new UnsupportedOperationException();
  }

  @Override
  public Query<T> withId(String id)
  {
    return withOverriddenContext(ImmutableMap.of(BaseQuery.QUERY_ID, id));
  }

  @Nullable
  @Override
  public String getId()
  {
    return context().getString(BaseQuery.QUERY_ID);
  }

  @Override
  public Query<T> withSubQueryId(String subQueryId)
  {
    return withOverriddenContext(ImmutableMap.of(BaseQuery.SUB_QUERY_ID, subQueryId));
  }

  @Override
  public Query<T> withDefaultSubQueryId()
  {
    // This is never used... why does it even exist!?
    throw new UnsupportedOperationException();
  }

  @Nullable
  @Override
  public String getSubQueryId()
  {
    return context().getString(BaseQuery.SUB_QUERY_ID);
  }

  @Override
  public Query<T> withDataSource(DataSource dataSource)
  {
    return new SingleQueryUnionQuery<T>(
        sources,
        subQuery.withDataSource(dataSource),
        context
    );
  }

  @Override
  public VirtualColumns getVirtualColumns()
  {
    // Should never be called on UnionQuery
    throw new UnsupportedOperationException();
  }

  @Nullable
  @Override
  public Set<String> getRequiredColumns()
  {
    return subQuery.getRequiredColumns();
  }
}
