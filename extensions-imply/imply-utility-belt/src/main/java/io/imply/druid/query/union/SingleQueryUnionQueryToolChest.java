/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.query.union;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Function;
import com.google.common.collect.Lists;
import com.google.inject.Inject;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.UOE;
import org.apache.druid.java.util.common.guava.MergeSequence;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.guava.Sequences;
import org.apache.druid.query.CacheStrategy;
import org.apache.druid.query.GenericQueryMetricsFactory;
import org.apache.druid.query.Queries;
import org.apache.druid.query.Query;
import org.apache.druid.query.QueryMetrics;
import org.apache.druid.query.QueryRunner;
import org.apache.druid.query.QueryToolChest;
import org.apache.druid.query.QueryToolChestWarehouse;
import org.apache.druid.query.aggregation.MetricManipulationFn;
import org.apache.druid.segment.column.RowSignature;

import javax.annotation.Nullable;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;
import java.util.function.BinaryOperator;
import java.util.function.Supplier;

@SuppressWarnings({"unchecked", "rawtypes"})
public class SingleQueryUnionQueryToolChest extends QueryToolChest
{
  private final GenericQueryMetricsFactory metricsFactory;
  private final Supplier<QueryToolChestWarehouse> warehouseSupplier;

  /**
   * Constructor for the SingleQueryUnionQueryToolChest.  Given that this tool chest depends on being able to
   * call things for the tool chests of its sub queries, we have to be careful about circular dependencies
   * with the QueryToolChestWarehouse.  We avoid this by injecting a Supplier instead of the actual instance,
   * which means that we can construct the Warehouse first and then return that reference from the Supplier.
   *
   * @param metricsFactory    metrics factory
   * @param warehouseSupplier supplier to avoid a circular dependency
   */
  @Inject
  public SingleQueryUnionQueryToolChest(
      GenericQueryMetricsFactory metricsFactory,
      Supplier<QueryToolChestWarehouse> warehouseSupplier
  )
  {

    this.metricsFactory = metricsFactory;
    this.warehouseSupplier = warehouseSupplier;
  }

  @Override
  public QueryRunner mergeResults(QueryRunner baseRunner)
  {
    return (queryPlus, responseContext) -> {
      final SingleQueryUnionQuery<?> unionQuery = (SingleQueryUnionQuery<?>) queryPlus.getQuery();
      final QueryToolChest<?, ? extends Query<?>> subToolchest = warehouseSupplier
          .get()
          .getToolChest(unionQuery.getSubQuery());

      return subToolchest.postMergeQueryDecoration((QueryRunner) subToolchest.mergeResults(
          (subQueryPlus, subResponseContext) -> {
            final List<UnionSubQueryOverrides> sources = unionQuery.getSources();
            final Query<?> subQuery = subQueryPlus.getQuery();
            QueryRunner subRunner = subToolchest.preMergeQueryDecoration(baseRunner);

            if (sources.size() == 1) {
              final UnionSubQueryOverrides onlySource = sources.get(0);
              return subRunner.run(
                  subQueryPlus.withQuery(
                      Queries.withBaseDataSource(subQuery, onlySource.maybeFilteredDataSource())
                             .withQuerySegmentSpec(onlySource.getQuerySegmentSpec())
                             .withOverriddenContext(unionQuery.getContext())
                  ),
                  subResponseContext
              );
            } else {
              String subQueryId = unionQuery.getSubQueryId();
              if (subQueryId == null) {
                subQueryId = "";
              } else {
                subQueryId += ".";
              }

              final AtomicInteger counter = new AtomicInteger(0);
              final String finalSubQueryId = subQueryId;
              return new MergeSequence(
                  subQuery.getResultOrdering(),
                  Sequences.simple(
                      Lists.transform(
                          sources,
                          source -> {
                            int sourceIndex = counter.getAndIncrement();
                            final String mySubQueryId = StringUtils.format(
                                "%s%s-%d",
                                finalSubQueryId,
                                source.getDataSource(),
                                sourceIndex
                            );

                            return subRunner.run(
                                subQueryPlus.withQuery(
                                    Queries.withBaseDataSource(subQuery, source.maybeFilteredDataSource())
                                           .withQuerySegmentSpec(source.getQuerySegmentSpec())
                                           .withOverriddenContext(unionQuery.getContext())
                                           .withSubQueryId(mySubQueryId)
                                ),
                                subResponseContext
                            );
                          }
                      )
                  )
              );
            }
          }
      )).run(queryPlus.withQuery(unionQuery.getSubQuery()), responseContext);
    };
  }

  @Nullable
  @Override
  public BinaryOperator createMergeFn(Query query)
  {
    throw new UOE("Should never be called because the mergeResults doesn't need it");
  }

  @Override
  public Comparator createResultComparator(Query query)
  {
    throw new UOE("Should never be called because the mergeResults doesn't need it");
  }

  @Override
  public QueryMetrics makeMetrics(Query query)
  {
    return metricsFactory.makeMetrics(query);
  }

  @Override
  public Function makePreComputeManipulatorFn(
      Query query, MetricManipulationFn fn
  )
  {
    throw new UOE("Should never be called because it should be pushed down to the subQuery");
  }

  @Override
  public Function makePostComputeManipulatorFn(
      Query query, MetricManipulationFn fn
  )
  {
    return withSubQueryAndToolchest(
        query,
        (subQuery, toolchest) -> toolchest.makePostComputeManipulatorFn(subQuery, fn)
    );
  }

  @SuppressWarnings("ReturnOfNull")
  @Override
  public TypeReference getResultTypeReference()
  {
    // This should never actually be needed as the UnionQuery itself is never actually passed across the wire.
    // This type reference is used to deserialize results from downstream servers, the subquery is the one
    // that would actually be passed down and that will have this actually implemented meaningfully.
    return null;
  }

  @Nullable
  @Override
  public CacheStrategy getCacheStrategy(Query query)
  {
    return null;
  }

  @Override
  public QueryRunner preMergeQueryDecoration(QueryRunner runner)
  {
    return (queryPlus, responseContext) -> {
      final Query alreadySwappedQuery = queryPlus.getQuery();

      // This will already have the query swapped out for the subQuery, double check that we aren't getting
      // a SingleQueryUnion query here as if we are, we are likely to be in an infinite loop.
      if (alreadySwappedQuery instanceof SingleQueryUnionQuery) {
        throw new UOE("Throwing exception to avoid infinite loop, should have subquery here.");
      }

      return warehouseSupplier
          .get()
          .getToolChest(alreadySwappedQuery)
          .preMergeQueryDecoration(runner)
          .run(queryPlus, responseContext);
    };
  }

  @Override
  public QueryRunner postMergeQueryDecoration(QueryRunner runner)
  {
    return runner;
  }

  @Override
  public List filterSegments(Query query, List segments)
  {
    throw new UOE("Should never be called because it should be pushed down to the subQuery");
  }

  @Override
  public RowSignature resultArraySignature(Query query)
  {
    return withSubQueryAndToolchest(
        query,
        (subQuery, toolchest) -> toolchest.resultArraySignature(subQuery)
    );
  }

  @Override
  public Sequence<Object[]> resultsAsArrays(
      Query query, Sequence resultSequence
  )
  {
    return withSubQueryAndToolchest(
        query,
        (subQuery, toolchest) -> toolchest.resultsAsArrays(subQuery, resultSequence)
    );
  }

  @Override
  public ObjectMapper decorateObjectMapper(
      ObjectMapper objectMapper, Query query
  )
  {
    return withSubQueryAndToolchest(
        query,
        (subQuery, toolchest) -> toolchest.decorateObjectMapper(objectMapper, subQuery)
    );
  }

  @Override
  public boolean canPerformSubquery(Query query)
  {
    return withSubQueryAndToolchest(query, (subQuery, toolchest) -> toolchest.canPerformSubquery(query));
  }

  private <T> T withSubQueryAndToolchest(Query query, BiFunction<Query, QueryToolChest, T> fn)
  {
    final QueryToolChestWarehouse warehouse = warehouseSupplier.get();
    final Query subQuery = ((SingleQueryUnionQuery) query).getSubQuery();
    final Object retVal = fn.apply(subQuery, warehouse.getToolChest(subQuery));
    // This is a very weird casting dance that the IDE will highlight as not being necessary.  But, jdk8 fails
    // compilation if we don't do it this way, so this is how we do it.
    return (T) retVal;
  }
}
