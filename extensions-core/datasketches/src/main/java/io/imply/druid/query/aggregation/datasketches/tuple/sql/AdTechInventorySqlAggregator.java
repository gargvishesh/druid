/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.query.aggregation.datasketches.tuple.sql;

import com.google.common.collect.ImmutableList;
import io.imply.druid.query.aggregation.datasketches.tuple.AdTechInventoryAggregatorFactory;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.Optionality;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.datasketches.tuple.ArrayOfDoublesSketchModule;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.ColumnTypeFactory;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.sql.calcite.aggregation.Aggregation;
import org.apache.druid.sql.calcite.aggregation.Aggregations;
import org.apache.druid.sql.calcite.aggregation.SqlAggregator;
import org.apache.druid.sql.calcite.expression.DruidExpression;
import org.apache.druid.sql.calcite.expression.Expressions;
import org.apache.druid.sql.calcite.planner.Calcites;
import org.apache.druid.sql.calcite.planner.PlannerContext;
import org.apache.druid.sql.calcite.rel.VirtualColumnRegistry;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;

/**
 * AdTechInventorySqlAggregator represents the Druid SQL Aggregator function AD_TECH_INVENTORY that helps in creating
 * the {@link AdTechInventoryAggregatorFactory}
 * This aggregation can be utilized as follows:
 * 1. AD_TECH_INVENTORY(userColumn, impressionColumn)
 * 2. AD_TECH_INVENTORY(userColumn, impressionColumn, frequencyCap)
 * 3. AD_TECH_INVENTORY(userColumn, impressionColumn, frequencyCap, sampleSize)
 * 3. AD_TECH_INVENTORY(complexColumn, impressionColumn, frequencyCap, sampleSize)
 */
public class AdTechInventorySqlAggregator implements SqlAggregator
{
  private static final Logger log = new Logger(AdTechInventorySqlAggregator.class);
  private static final SqlAggFunction FUNCTION_INSTANCE = new AdTechInventorySqlAggFunction();
  public static final String NAME = "AD_TECH_INVENTORY";

  @Override
  public SqlAggFunction calciteFunction()
  {
    return FUNCTION_INSTANCE;
  }

  @Nullable
  @Override
  public Aggregation toDruidAggregation(
      PlannerContext plannerContext,
      RowSignature rowSignature,
      VirtualColumnRegistry virtualColumnRegistry,
      RexBuilder rexBuilder,
      String name,
      AggregateCall aggregateCall,
      Project project,
      List<Aggregation> existingAggregations,
      boolean finalizeAggregations
  )
  {
    List<Integer> argList = aggregateCall.getArgList();


    if (argList.size() < 2 || argList.size() > 4) {
      // log statement is not required, since Calcite throws an error of similar kind
      List<String> argListExpression = new ArrayList<>();
      for (Integer arg : argList) {
        DruidExpression expression = extractColumnExpressionFromProject(rowSignature, project, arg, plannerContext);
        argListExpression.add(expression.getExpression());
      }

      log.warn("Arguments passed to " + NAME + " aggregation: " + String.join(", ", argListExpression));
      return null;
    }

    Aggregation aggregationIfPrebuiltColumn = aggregationWhenPrebuiltColumn(
        plannerContext,
        rowSignature,
        virtualColumnRegistry,
        name,
        aggregateCall,
        project
    );
    if (aggregationIfPrebuiltColumn != null) {
      return aggregationIfPrebuiltColumn;
    }

    RexNode node = Expressions.fromFieldAccess(rowSignature, project, argList.get(0));
    DruidExpression userColumn = Expressions.toDruidExpression(plannerContext, rowSignature, node);
    if (userColumn == null) {
      return null;
    }
    String userColumnName = extractColumnNameFromDruidExpression(
        userColumn,
        virtualColumnRegistry,
        plannerContext,
        Calcites.getColumnTypeForRelDataType(node.getType())
    );


    // fetch impression column name
    DruidExpression impressionColumn = Aggregations.toDruidExpressionForNumericAggregator(
        plannerContext,
        rowSignature,
        Expressions.fromFieldAccess(
            rowSignature,
            project,
            argList.get(1)
        )
    );
    if (impressionColumn == null) {
      return null;
    }
    String impressionColumnName = extractColumnNameFromDruidExpression(
        impressionColumn,
        virtualColumnRegistry,
        plannerContext,
        ColumnType.DOUBLE
    );

    // check if frequency cap is provided
    Integer frequencyCap = null;
    if (argList.size() >= 3) {
      frequencyCap = extractIntFromProject(rowSignature, project, argList.get(2));
      if (frequencyCap == null) {
        return null;
      }
    }

    // check if sample size is provided
    Integer sampleSize = null;
    if (argList.size() >= 4) {
      sampleSize = extractIntFromProject(rowSignature, project, argList.get(3));
      if (sampleSize == null) {
        return null;
      }
    }

    AggregatorFactory aggregatorFactory = AdTechInventoryAggregatorFactory.getAdTechImpressionCountAggregatorFactory(
        StringUtils.format("%s:agg", name),
        userColumnName,
        impressionColumnName,
        null,
        frequencyCap,
        sampleSize
    );

    return Aggregation.create(ImmutableList.of(aggregatorFactory), null);
  }

  @Nullable
  private Aggregation aggregationWhenPrebuiltColumn(
      PlannerContext plannerContext,
      RowSignature rowSignature,
      VirtualColumnRegistry virtualColumnRegistry,
      String name,
      AggregateCall aggregateCall,
      Project project
  )
  {
    List<Integer> argList = aggregateCall.getArgList();

    // This check works since all the signatures of the call to complex columns have 2 arguments
    if (argList.size() != 2) {
      return null;
    }

    DruidExpression columnExpression = extractColumnExpressionFromProject(
        rowSignature,
        project,
        argList.get(0),
        plannerContext
    );
    boolean isComplexColumn =
        columnExpression.isDirectColumnAccess()
        && rowSignature.getColumnType(columnExpression.getDirectColumn())
                       .map(type -> type.equals(ColumnTypeFactory
                                                    .getInstance()
                                                    .ofComplex(ArrayOfDoublesSketchModule.ARRAY_OF_DOUBLES_SKETCH))
                       )
                       .orElse(false);
    if (!isComplexColumn) {
      return null;
    }
    String columnName = extractColumnNameFromDruidExpression(
        columnExpression,
        virtualColumnRegistry,
        plannerContext,
        null
    );
    if (columnName == null) {
      return null;
    }

    Integer frequencyCap = extractIntFromProject(rowSignature, project, argList.get(1));
    if (frequencyCap == null) {
      return null;
    }

    AggregatorFactory aggregatorFactory = AdTechInventoryAggregatorFactory.getAdTechImpressionCountAggregatorFactory(
        StringUtils.format("%s:agg", name),
        null,
        null,
        columnName,
        frequencyCap,
        null
    );

    return Aggregation.create(ImmutableList.of(aggregatorFactory), null);
  }

  @Nullable
  String extractColumnNameFromDruidExpression(
      DruidExpression column,
      VirtualColumnRegistry virtualColumnRegistry,
      PlannerContext plannerContext,
      // columnType is required when we know we want to extract the virtual column name
      // This arg can be null in case of direct column access
      @Nullable ColumnType columnType
  )
  {
    if (column.isDirectColumnAccess()) {
      return column.getDirectColumn();
    }

    if (columnType == null) {
      return null; // Shouldn't reach here if called correctly
    }

    return virtualColumnRegistry.getOrCreateVirtualColumnForExpression(
        plannerContext,
        column,
        columnType
    ).getOutputName();
  }

  DruidExpression extractColumnExpressionFromProject(
      RowSignature rowSignature,
      Project project,
      Integer arg,
      PlannerContext plannerContext
  )
  {
    return Expressions.toDruidExpression(
        plannerContext,
        rowSignature,
        Expressions.fromFieldAccess(rowSignature, project, arg)
    );
  }

  @Nullable
  Integer extractIntFromProject(RowSignature rowSignature, Project project, int arg)
  {
    final RexNode node = Expressions.fromFieldAccess(rowSignature, project, arg);
    if (!node.isA(SqlKind.LITERAL)) {
      return null;
    }

    return ((Number) RexLiteral.value(node)).intValue();
  }

  private static class AdTechInventorySqlAggFunction extends SqlAggFunction
  {
    private static final String SIGNATURE1 = "'" + NAME + "'(userColumn, impressionColumn, frequencyCap, sampleSize)";
    private static final String SIGNATURE2 = "'" + NAME + "'(userColumn, impressionColumn, frequencyCap)";
    private static final String SIGNATURE3 = "'" + NAME + "'(userColumn, impressionColumn)";
    private static final String SIGNATURE4 = "'" + NAME + "'(complexColumn, frequencyCap)";

    AdTechInventorySqlAggFunction()
    {
      super(
          NAME,
          null,
          SqlKind.OTHER_FUNCTION,
          ReturnTypes.explicit(SqlTypeName.OTHER),
          null,
          OperandTypes.or(
              // AD_TECH_INVENTORY(userColumn, impressionColumn, frequencyCap, sampleSize)
              OperandTypes.and(
                  OperandTypes.sequence(
                      SIGNATURE1,
                      OperandTypes.ANY,
                      OperandTypes.ANY,
                      OperandTypes.POSITIVE_INTEGER_LITERAL,
                      OperandTypes.POSITIVE_INTEGER_LITERAL
                  ),
                  OperandTypes.family(
                      SqlTypeFamily.ANY,
                      SqlTypeFamily.ANY,
                      SqlTypeFamily.EXACT_NUMERIC,
                      SqlTypeFamily.EXACT_NUMERIC
                  )
              ),
              // AD_TECH_INVENTORY(userColumn, impressionColumn, frequencyCap)
              OperandTypes.and(
                  OperandTypes.sequence(
                      SIGNATURE2,
                      OperandTypes.ANY,
                      OperandTypes.ANY,
                      OperandTypes.POSITIVE_INTEGER_LITERAL
                  ),
                  OperandTypes.family(
                      SqlTypeFamily.ANY,
                      SqlTypeFamily.ANY,
                      SqlTypeFamily.EXACT_NUMERIC
                  )
              ),
              // AD_TECH_INVENTORY(userColumn, impressionColumn)
              OperandTypes.and(
                  OperandTypes.sequence(
                      SIGNATURE3,
                      OperandTypes.ANY,
                      OperandTypes.ANY
                  ),
                  OperandTypes.family(
                      SqlTypeFamily.ANY,
                      SqlTypeFamily.ANY
                  )
              ),
              // AD_TECH_INVENTORY(complexColumn, frequencyCap)
              OperandTypes.and(
                  OperandTypes.sequence(
                      SIGNATURE4,
                      OperandTypes.ANY,
                      OperandTypes.POSITIVE_INTEGER_LITERAL
                  ),
                  OperandTypes.family(
                      SqlTypeFamily.ANY,
                      SqlTypeFamily.EXACT_NUMERIC
                  )
              )
          ),
          SqlFunctionCategory.USER_DEFINED_FUNCTION,
          false,
          false,
          Optionality.IGNORED // TODO: Check these parameters
      );
    }
  }
}
