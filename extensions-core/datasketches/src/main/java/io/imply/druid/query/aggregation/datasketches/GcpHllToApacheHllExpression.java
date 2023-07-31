/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.query.aggregation.datasketches;

import com.google.zetasketch.internal.ByteSlice;
import com.google.zetasketch.internal.DifferenceDecoder;
import com.google.zetasketch.internal.hllplus.Encoding;
import com.google.zetasketch.internal.hllplus.State;
import com.google.zetasketch.shaded.com.google.protobuf.CodedInputStream;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.datasketches.hll.HllSketch;
import org.apache.datasketches.hll.ImplyHllSketchCompat;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.math.expr.Expr;
import org.apache.druid.math.expr.ExprEval;
import org.apache.druid.math.expr.ExprMacroTable;
import org.apache.druid.math.expr.ExpressionType;
import org.apache.druid.query.aggregation.datasketches.hll.HllSketchHolder;
import org.apache.druid.query.aggregation.datasketches.hll.HllSketchMergeAggregatorFactory;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.sql.calcite.expression.DruidExpression;
import org.apache.druid.sql.calcite.expression.OperatorConversions;
import org.apache.druid.sql.calcite.expression.SqlOperatorConversion;
import org.apache.druid.sql.calcite.planner.PlannerContext;
import org.apache.druid.sql.calcite.table.RowSignatures;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.Base64;
import java.util.List;

public class GcpHllToApacheHllExpression implements ExprMacroTable.ExprMacro
{
  public static final Base64.Decoder BASE64 = Base64.getDecoder();

  public static final ExpressionType HLL_TYPE = ExpressionType.fromColumnTypeStrict(
      HllSketchMergeAggregatorFactory.TYPE
  );

  public static final String FN_NAME = "gbq_hll_to_ds_hll";

  @Override
  public String name()
  {
    return FN_NAME;
  }

  @Override
  public Expr apply(List<Expr> args)
  {
    validationHelperCheckAnyOfArgumentCount(args, 1);

    class HLLExtractExpr extends ExprMacroTable.BaseScalarUnivariateMacroFunctionExpr
    {
      private HLLExtractExpr(Expr arg)
      {
        super(FN_NAME, arg);
      }

      @Override
      public ExprEval<?> eval(ObjectBinding objectBinding)
      {
        ExprEval<?> hllppExpr = args.get(0).eval(objectBinding);

        if (hllppExpr.value() == null) {
          return ExprEval.ofComplex(HLL_TYPE, null);
        }
        Object serializedValue = hllppExpr.value();
        if (serializedValue instanceof String) {
          try {
            serializedValue = BASE64.decode((String) serializedValue);
          }
          catch (IllegalArgumentException e) {
            return ExprEval.ofComplex(HLL_TYPE, null);
          }
        }

        if (serializedValue instanceof byte[]) {
          HllSketch sketch;
          try {
            sketch = apacheHllFromZetaBytes((byte[]) serializedValue);
          }
          catch (IOException e) {
            throw new RuntimeException(e);
          }
          return ExprEval.ofComplex(HLL_TYPE, sketch == null ? null : HllSketchHolder.of(sketch));
        }
        return ExprEval.ofComplex(HLL_TYPE, null);
      }

      @Override
      public Expr visit(Shuttle shuttle)
      {
        return shuttle.visit(apply(shuttle.visitAll(args)));
      }

      @Override
      public ExpressionType getOutputType(InputBindingInspector inspector)
      {
        return HLL_TYPE;
      }

    }
    return new HLLExtractExpr(args.get(0));
  }

  private static HllSketch apacheHllFromZetaBytes(byte[] zetaBytes) throws IOException
  {
    CodedInputStream cis = CodedInputStream.newInstance(zetaBytes);
    cis.enableAliasing(true);
    State state = new State();
    state.parse(cis);

    if (state.hasSparseData()) {
      return ImplyHllSketchCompat.makeWithCoupons(
          state.precision,
          new ImplyHllSketchCompat.IndexValuePairs()
          {
            private final DifferenceDecoder decode = new DifferenceDecoder(state.sparseData);
            private final Encoding.Sparse encoding = new Encoding.Sparse(state.precision, state.sparsePrecision);

            int currInt = -1;

            @Override
            public boolean nextValid()
            {
              if (decode.hasNext()) {
                currInt = decode.nextInt();
                return true;
              }
              return false;
            }

            @Override
            public int getIndex()
            {
              return encoding.decodeNormalIndex(currInt);
            }

            @Override
            public int getValue()
            {
              return encoding.decodeNormalRhoW(currInt);
            }
          }
      );
    } else if (state.hasData()) {
      return ImplyHllSketchCompat.makeWithCoupons(
          state.precision,
          new ImplyHllSketchCompat.IndexValuePairs()
          {
            private final ByteSlice stateData = state.data;
            private final byte[] arr = stateData.array();
            private final int offset = stateData.arrayOffset();

            final int denseBytes = arr.length - offset;

            int index = -1;

            @Override
            public boolean nextValid()
            {
              return ++index < denseBytes;
            }

            @Override
            public int getIndex()
            {
              return index;
            }

            @Override
            public int getValue()
            {
              return arr[offset + index];
            }
          }
      );
    } else {
      return null;
    }
  }

  public static class HLLPPToHLLConversion implements SqlOperatorConversion
  {

    private static final SqlFunction SQL_FUNCTION = OperatorConversions
        .operatorBuilder(StringUtils.toUpperCase(FN_NAME))
        .operandTypes(SqlTypeFamily.ANY)
        .returnTypeInference(
            opBinding -> RowSignatures.makeComplexType(
                opBinding.getTypeFactory(),
                HllSketchMergeAggregatorFactory.TYPE,
                true
            )
        )
        .build();

    @Override
    public SqlOperator calciteOperator()
    {
      return SQL_FUNCTION;
    }

    @Nullable
    @Override
    public DruidExpression toDruidExpression(PlannerContext plannerContext, RowSignature rowSignature, RexNode rexNode)
    {
      return OperatorConversions.convertDirectCall(plannerContext, rowSignature, rexNode, FN_NAME);
    }
  }
}
