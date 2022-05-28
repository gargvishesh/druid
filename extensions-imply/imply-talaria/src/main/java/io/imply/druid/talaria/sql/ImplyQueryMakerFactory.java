/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.talaria.sql;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import io.imply.druid.sql.TalariaParserUtils;
import io.imply.druid.talaria.querykit.QueryKitUtils;
import io.imply.druid.talaria.rpc.indexing.OverlordServiceClient;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.tools.ValidationException;
import org.apache.calcite.util.Pair;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.granularity.Granularity;
import org.apache.druid.segment.column.ColumnHolder;
import org.apache.druid.server.QueryLifecycleFactory;
import org.apache.druid.sql.calcite.parser.DruidSqlInsert;
import org.apache.druid.sql.calcite.planner.Calcites;
import org.apache.druid.sql.calcite.planner.PlannerContext;
import org.apache.druid.sql.calcite.run.NativeQueryMaker;
import org.apache.druid.sql.calcite.run.QueryMaker;
import org.apache.druid.sql.calcite.run.QueryMakerFactory;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

// Note: This package and class name is hard-coded into
// AsyncQueryResource: if the names change, change that
// class as well.
public class ImplyQueryMakerFactory implements QueryMakerFactory
{
  public static final String TYPE = "imply";

  private final QueryLifecycleFactory queryLifecycleFactory;
  private final OverlordServiceClient overlordClient;
  private final ObjectMapper jsonMapper;

  @Inject
  public ImplyQueryMakerFactory(
      final QueryLifecycleFactory queryLifecycleFactory,
      final OverlordServiceClient overlordClient,
      final ObjectMapper jsonMapper
  )
  {
    this.queryLifecycleFactory = queryLifecycleFactory;
    this.overlordClient = overlordClient;
    this.jsonMapper = jsonMapper;
  }

  @Override
  public QueryMaker buildForSelect(
      final RelRoot relRoot,
      final PlannerContext plannerContext
  ) throws ValidationException
  {
    if (TalariaParserUtils.isTalaria(plannerContext)) {
      validateTalariaSelect(relRoot.fields, plannerContext);

      return new TalariaQueryMaker(
          null,
          overlordClient,
          plannerContext,
          jsonMapper,
          relRoot.fields,
          getTalariaStructType(relRoot.rel.getCluster().getTypeFactory())
      );
    } else {
      return new NativeQueryMaker(
          queryLifecycleFactory,
          plannerContext,
          jsonMapper,
          relRoot.fields,
          relRoot.validatedRowType
      );
    }
  }

  @Override
  public QueryMaker buildForInsert(
      final String targetDataSource,
      final RelRoot relRoot,
      final PlannerContext plannerContext
  ) throws ValidationException
  {
    if (TalariaParserUtils.isTalaria(plannerContext)) {
      validateTalariaInsert(relRoot.rel, relRoot.fields, plannerContext);

      return new TalariaQueryMaker(
          targetDataSource,
          overlordClient,
          plannerContext,
          jsonMapper,
          relRoot.fields,
          getTalariaStructType(relRoot.rel.getCluster().getTypeFactory())
      );
    } else {
      throw new ValidationException("Cannot execute INSERT queries in standard query mode.");
    }
  }

  private static void validateTalariaSelect(
      final List<Pair<Integer, String>> fieldMappings,
      final PlannerContext plannerContext
  ) throws ValidationException
  {
    validateNoDuplicateAliases(fieldMappings);

    if (plannerContext.getQueryContext().containsKey(DruidSqlInsert.SQL_INSERT_SEGMENT_GRANULARITY)) {
      throw new ValidationException(
          StringUtils.format("Cannot use \"%s\" without INSERT", DruidSqlInsert.SQL_INSERT_SEGMENT_GRANULARITY)
      );
    }
  }

  private static void validateTalariaInsert(
      final RelNode rootRel,
      final List<Pair<Integer, String>> fieldMappings,
      final PlannerContext plannerContext
  ) throws ValidationException
  {
    validateNoDuplicateAliases(fieldMappings);

    // Find the __time field.
    int timeFieldIndex = -1;

    for (final Pair<Integer, String> field : fieldMappings) {
      if (field.right.equals(ColumnHolder.TIME_COLUMN_NAME)) {
        timeFieldIndex = field.left;

        // Validate the __time field has the proper type.
        final SqlTypeName timeType = rootRel.getRowType().getFieldList().get(field.left).getType().getSqlTypeName();
        if (timeType != SqlTypeName.TIMESTAMP) {
          throw new ValidationException(
              StringUtils.format(
                  "Field \"%s\" must be of type TIMESTAMP (was %s)",
                  ColumnHolder.TIME_COLUMN_NAME,
                  timeType
              )
          );
        }
      }
    }

    // Validate that if segmentGranularity is not ALL then there is also a __time field.
    final Granularity segmentGranularity;

    try {
      segmentGranularity = QueryKitUtils.getSegmentGranularityFromContext(
          plannerContext.getQueryContext().getMergedParams()
      );
    }
    catch (Exception e) {
      throw new ValidationException(
          StringUtils.format(
              "Invalid segmentGranularity: %s",
              plannerContext.getQueryContext().get(DruidSqlInsert.SQL_INSERT_SEGMENT_GRANULARITY)
          ),
          e
      );
    }

    final boolean hasSegmentGranularity = !Granularities.ALL.equals(segmentGranularity);

    // Validate that the query does not have an inappropriate LIMIT or OFFSET. LIMIT prevents gathering result key
    // statistics, which INSERT execution logic depends on. (In QueryKit, LIMIT disables statistics generation and
    // funnels everything through a single partition.)
    validateLimitAndOffset(rootRel, !hasSegmentGranularity);

    if (hasSegmentGranularity && timeFieldIndex < 0) {
      throw new ValidationException(
          StringUtils.format(
              "INSERT queries with segment granularity other than \"all\" must have a \"%s\" field.",
              ColumnHolder.TIME_COLUMN_NAME
          )
      );
    }
  }

  /**
   * SQL allows multiple output columns with the same name, but Talaria doesn't.
   */
  private static void validateNoDuplicateAliases(final List<Pair<Integer, String>> fieldMappings)
      throws ValidationException
  {
    final Set<String> aliasesSeen = new HashSet<>();

    for (final Pair<Integer, String> field : fieldMappings) {
      if (!aliasesSeen.add(field.right)) {
        throw new ValidationException("Duplicate field in SELECT: " + field.right);
      }
    }
  }

  private static void validateLimitAndOffset(final RelNode topRel, final boolean limitOk) throws ValidationException
  {
    Sort sort = null;

    if (topRel instanceof Sort) {
      sort = (Sort) topRel;
    } else if (topRel instanceof Project) {
      // Look for Project after a Sort, then validate the sort.
      final Project project = (Project) topRel;
      if (project.isMapping()) {
        final RelNode projectInput = project.getInput();
        if (projectInput instanceof Sort) {
          sort = (Sort) projectInput;
        }
      }
    }

    if (sort != null && sort.fetch != null && !limitOk) {
      // Found an outer LIMIT that is not allowed.
      throw new ValidationException(
          StringUtils.format(
              "INSERT queries cannot end with LIMIT unless %s is \"all\".",
              DruidSqlInsert.SQL_INSERT_SEGMENT_GRANULARITY
          )
      );
    }

    if (sort != null && sort.offset != null) {
      // Found an outer OFFSET that is not allowed.
      throw new ValidationException("INSERT queries cannot end with OFFSET.");
    }
  }

  private static RelDataType getTalariaStructType(RelDataTypeFactory typeFactory)
  {
    return typeFactory.createStructType(
        ImmutableList.of(Calcites.createSqlType(typeFactory, SqlTypeName.VARCHAR)),
        ImmutableList.of("TASK")
    );
  }
}
