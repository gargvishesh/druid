/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.imply.druid.inet;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.Scopes;
import com.google.inject.TypeLiteral;
import com.google.inject.multibindings.Multibinder;
import io.imply.druid.inet.expression.IpAddressExpressions;
import io.imply.druid.inet.sql.IpAddressSqlOperatorConversions;
import io.imply.druid.license.ImplyLicenseManager;
import org.apache.druid.guice.ExpressionModule;
import org.apache.druid.guice.LazySingleton;
import org.apache.druid.math.expr.ExprMacroTable;
import org.apache.druid.sql.calcite.expression.SqlOperatorConversion;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

@RunWith(MockitoJUnitRunner.class)
public class IpAddressModuleTest
{
  private static final Set<Class<? extends ExprMacroTable.ExprMacro>> IP_ADDRESS_EXPRESSIONS = ImmutableSet.of(
      IpAddressExpressions.AddressParseExprMacro.class,
      IpAddressExpressions.AddressTryParseExprMacro.class,
      IpAddressExpressions.StringifyExprMacro.class,
      IpAddressExpressions.PrefixExprMacro.class,
      IpAddressExpressions.MatchExprMacro.class
  );

  private static final Set<Class<? extends SqlOperatorConversion>> IP_ADDRESS_SQL_OPERATORS = ImmutableSet.of(
      IpAddressSqlOperatorConversions.AddressParseOperatorConversion.class,
      IpAddressSqlOperatorConversions.AddressTryParseOperatorConversion.class,
      IpAddressSqlOperatorConversions.StringifyOperatorConversion.class,
      IpAddressSqlOperatorConversions.PrefixOperatorConversion.class,
      IpAddressSqlOperatorConversions.MatchOperatorConversion.class
  );

  @Mock
  private ImplyLicenseManager implyLicenseManager;

  @Mock
  private ExprMacroTable.ExprMacro exprMacro;
  @Mock
  private SqlOperatorConversion sqlOperatorConversion;

  private Set<SqlOperatorConversion> sqlOperatorConversions;
  private Injector injector;
  private IpAddressModule target;

  @Before
  public void setup()
  {
    sqlOperatorConversions = new HashSet<>();
    Mockito.when(implyLicenseManager.isFeatureEnabled(IpAddressModule.FEATURE_NAME)).thenReturn(false);
    target = new IpAddressModule();
    target.setImplyLicenseManager(implyLicenseManager);
  }

  @Test
  public void testGetJacksonModulesShouldReturnEmptyList()
  {
    injector = createInjector();
    Assert.assertEquals(Collections.emptyList(), target.getJacksonModules());
  }

  @Test
  public void testExpressionsShouldNotBeBound()
  {
    injector = createInjector();
    Set<ExprMacroTable.ExprMacro> exprMacros =
        injector.getInstance(Key.get(new TypeLiteral<Set<ExprMacroTable.ExprMacro>>()
        {
        }));
    Set<Class<? extends ExprMacroTable.ExprMacro>> expressionMacroClasses =
        exprMacros.stream()
                  .map(ExprMacroTable.ExprMacro::getClass)
                  .collect(
                      Collectors.toSet());
    Assert.assertEquals(Collections.emptySet(), Sets.intersection(IP_ADDRESS_EXPRESSIONS, expressionMacroClasses));
  }

  @Test
  public void testExpressionsWhenLicenseEnabledShouldBeBound()
  {
    Mockito.when(implyLicenseManager.isFeatureEnabled(IpAddressModule.FEATURE_NAME)).thenReturn(true);
    injector = createInjector();
    Set<ExprMacroTable.ExprMacro> exprMacros =
        injector.getInstance(Key.get(new TypeLiteral<Set<ExprMacroTable.ExprMacro>>()
        {
        }));
    Set<Class<? extends ExprMacroTable.ExprMacro>> expressionMacroClasses =
        exprMacros.stream()
                  .map(ExprMacroTable.ExprMacro::getClass)
                  .collect(
                      Collectors.toSet());
    Assert.assertEquals(IP_ADDRESS_EXPRESSIONS, Sets.intersection(IP_ADDRESS_EXPRESSIONS, expressionMacroClasses));
  }

  @Test
  public void testSqlOperatorConversionsShouldNotBeBound()
  {
    injector = createInjector();
    Set<SqlOperatorConversion> sqlOperators =
        injector.getInstance(Key.get(new TypeLiteral<Set<SqlOperatorConversion>>()
        {
        }));
    Set<Class<? extends SqlOperatorConversion>> sqlOperatorClasses =
        sqlOperators.stream()
                  .map(SqlOperatorConversion::getClass)
                  .collect(
                      Collectors.toSet());
    Assert.assertEquals(Collections.emptySet(), Sets.intersection(IP_ADDRESS_SQL_OPERATORS, sqlOperatorClasses));
  }

  @Test
  public void testSqlOperatorConversionsWhenLicenseEnabledShouldBeBound()
  {
    Mockito.when(implyLicenseManager.isFeatureEnabled(IpAddressModule.FEATURE_NAME)).thenReturn(true);
    injector = createInjector();
    Set<SqlOperatorConversion> sqlOperators =
        injector.getInstance(Key.get(new TypeLiteral<Set<SqlOperatorConversion>>()
        {
        }));
    Set<Class<? extends SqlOperatorConversion>> sqlOperatorClasses =
        sqlOperators.stream()
                    .map(SqlOperatorConversion::getClass)
                    .collect(
                        Collectors.toSet());
    Assert.assertEquals(IP_ADDRESS_SQL_OPERATORS, Sets.intersection(IP_ADDRESS_SQL_OPERATORS, sqlOperatorClasses));
  }

  @Test
  public void testGetJacksonModulesWithLicenseEnabledShouldReturnEmptyList()
  {
    Mockito.when(implyLicenseManager.isFeatureEnabled(IpAddressModule.FEATURE_NAME)).thenReturn(true);
    injector = createInjector();
    Assert.assertEquals(1, target.getJacksonModules().size());
    Assert.assertEquals("IpAddressModule", target.getJacksonModules().get(0).getModuleName());
  }

  private Injector createInjector()
  {
    return Guice.createInjector(
        target,
        new ExpressionModule(),
        binder -> {
          binder.bindScope(LazySingleton.class, Scopes.SINGLETON);
          Multibinder.newSetBinder(binder, ExprMacroTable.ExprMacro.class)
                     .addBinding()
                     .toInstance(exprMacro);
          Multibinder.newSetBinder(binder, SqlOperatorConversion.class)
                     .addBinding()
                     .toInstance(sqlOperatorConversion);
        }
    );
  }
}
