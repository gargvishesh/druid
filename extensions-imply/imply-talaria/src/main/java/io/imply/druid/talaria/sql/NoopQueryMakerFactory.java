/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.talaria.sql;

import org.apache.calcite.rel.RelRoot;
import org.apache.druid.sql.calcite.planner.PlannerContext;
import org.apache.druid.sql.calcite.run.QueryMaker;
import org.apache.druid.sql.calcite.run.QueryMakerFactory;

public class NoopQueryMakerFactory implements QueryMakerFactory
{
  @Override
  public QueryMaker buildForSelect(RelRoot relRoot, PlannerContext plannerContext)
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public QueryMaker buildForInsert(String targetDataSource, RelRoot relRoot, PlannerContext plannerContext)
  {
    throw new UnsupportedOperationException();
  }
}
