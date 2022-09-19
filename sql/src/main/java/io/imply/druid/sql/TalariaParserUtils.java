/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.sql;

import org.apache.druid.sql.calcite.planner.PlannerContext;

/**
 * Utils class for Talaria related functions used for parsing queries.
 */
public class TalariaParserUtils
{
  public static final String CTX_MULTI_STAGE_QUERY = "multiStageQuery";

  public static boolean isTalaria(final PlannerContext plannerContext)
  {
    return plannerContext.getQueryContext().getAsBoolean(CTX_MULTI_STAGE_QUERY, false);
  }
}
