/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.segment.join;

import org.apache.druid.query.DataSource;
import org.apache.druid.query.GlobalTableDataSource;
import org.apache.druid.segment.join.JoinConditionAnalysis;
import org.apache.druid.segment.join.Joinable;
import org.apache.druid.segment.join.table.IndexedTableJoinable;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.Optional;

@RunWith(MockitoJUnitRunner.class)
public class IndexedTableJoinableFactoryTest
{
  private static final String EXISTING_TABLE_NAME = "EXISTING_TABLE_NAME";
  private static final String MISSING_TABLE_NAME = "MISSING_TABLE_NAME";

  @Mock
  private IndexedTableManager indexedTableManager;
  @Mock
  private CloseTableFirstReferenceCountingIndexedTable indexedTable;
  @Mock
  private JoinConditionAnalysis condition;
  @Mock
  private DataSource dataSource;
  @Mock
  private GlobalTableDataSource indexedTableDataSource;

  private IndexedTableJoinableFactory target;

  @Before
  public void setUp()
  {
    Mockito.when(indexedTableManager.getIndexedTable(EXISTING_TABLE_NAME)).thenReturn(indexedTable);
    Mockito.when(indexedTableManager.getIndexedTable(MISSING_TABLE_NAME)).thenReturn(null);
    Mockito.when(condition.canHashJoin()).thenReturn(true);
    Mockito.when(indexedTableDataSource.getName()).thenReturn(EXISTING_TABLE_NAME);
    target = new IndexedTableJoinableFactory(indexedTableManager);
  }

  @Test
  public void testBuildConditionCanNotHashJoinShouldReturnAbsent()
  {
    Mockito.doReturn(false).when(condition).canHashJoin();
    Optional<Joinable> joinable = target.build(indexedTableDataSource, condition);
    Assert.assertFalse(joinable.isPresent());
  }

  @Test(expected = ClassCastException.class)
  public void testBuildDataSourceIsNotIndexedTableDataSourceShouldThrowClassCastException()
  {
    Optional<Joinable> joinable = target.build(dataSource, condition);
    Assert.assertFalse(joinable.isPresent());
  }

  @Test
  public void testBuildMissingTableNameShouldReturnAbsent()
  {
    Mockito.doReturn(MISSING_TABLE_NAME).when(indexedTableDataSource).getName();
    Optional<Joinable> joinable = target.build(indexedTableDataSource, condition);
    Assert.assertFalse(joinable.isPresent());
  }

  @Test
  public void testBuildExistingTableNameShouldReturnIndexedTableJoinable()
  {
    Optional<Joinable> joinable = target.build(indexedTableDataSource, condition);
    Assert.assertTrue(joinable.isPresent());
    Assert.assertEquals(IndexedTableJoinable.class, joinable.get().getClass());
  }
}
