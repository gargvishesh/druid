/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.query.samplinggroupby.sql.calcite.rule;

import io.imply.druid.query.samplinggroupby.sql.calcite.rel.DruidSamplingGroupByQueryRel;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.core.Sample;
import org.apache.druid.sql.calcite.planner.PlannerContext;
import org.apache.druid.sql.calcite.rel.DruidRel;
import org.apache.druid.sql.calcite.rel.PartialDruidQuery;
import org.apache.druid.sql.calcite.rule.DruidRules;
import org.apache.druid.sql.calcite.rule.ExtensionCalciteRuleProvider;

/**
 * Rule to plan SamplingGroupByQuery. It only allows Sampling to occur in the logical plan after an aggregate has
 * occurred. Further, it plans to a SamplingGroupBy iff the sampling query is a native groupBy query.
 * The planning is done by creating an inner groupByQuery followed by sampling which then ends with an outer druid query.
 */
public class DruidSamplingGroupByQueryRule extends RelOptRule
{
  public DruidSamplingGroupByQueryRule()
  {
    super(
        operand(Sample.class, operandJ(DruidRel.class, null, DruidRules.CAN_BUILD_ON, any())),
        "SamplingGroupBy"
    );
  }

  @Override
  public boolean matches(final RelOptRuleCall call)
  {
    // Subquery must be a groupBy, so stage must be >= AGGREGATE.
    DruidRel druidRel = call.rel(call.getRelList().size() - 1);
    return druidRel.getPartialDruidQuery().stage().compareTo(PartialDruidQuery.Stage.AGGREGATE) >= 0;
  }

  @Override
  public void onMatch(RelOptRuleCall call)
  {
    Sample sample = call.rel(0);
    DruidRel druidRel = call.rel(1);

    DruidSamplingGroupByQueryRel samplingGroupByQueryRel = DruidSamplingGroupByQueryRel.create(
        druidRel,
        PartialDruidQuery.create(druidRel.getPartialDruidQuery().leafRel()),
        sample
    );
    if (samplingGroupByQueryRel.isValidDruidQuery()) {
      call.transformTo(samplingGroupByQueryRel);
    }
  }

  public static class DruidSamplingGroupByQueryRuleProvider implements ExtensionCalciteRuleProvider
  {
    @Override
    public RelOptRule getRule(PlannerContext plannerContext)
    {
      return new DruidSamplingGroupByQueryRule();
    }
  }
}
