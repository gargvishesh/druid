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
import org.junit.Test;

import static org.mockito.Mockito.mock;

public class NoopQueryMakerFactoryTest
{

  NoopQueryMakerFactory target = new NoopQueryMakerFactory();

  @Test(expected = UnsupportedOperationException.class)
  public void testBuildForSelect()
  {
    target.buildForSelect(mock(RelRoot.class), mock(PlannerContext.class));
  }

  @Test(expected = UnsupportedOperationException.class)
  public void testBuildForInsert()
  {
    target.buildForInsert("datasource1", mock(RelRoot.class), mock(PlannerContext.class));
  }
}
