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
import org.apache.druid.guice.ExpressionModule;
import org.apache.druid.guice.LazySingleton;
import org.apache.druid.jackson.JacksonModule;
import org.apache.druid.math.expr.ExprMacroTable;
import org.apache.druid.sql.calcite.expression.SqlOperatorConversion;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

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
      IpAddressExpressions.MatchExprMacro.class,
      IpAddressExpressions.SearchExprMacro.class,
      IpAddressExpressions.HostExprMacro.class
  );

  private static final Set<Class<? extends SqlOperatorConversion>> IP_ADDRESS_SQL_OPERATORS = ImmutableSet.of(
      IpAddressSqlOperatorConversions.AddressParseOperatorConversion.class,
      IpAddressSqlOperatorConversions.AddressTryParseOperatorConversion.class,
      IpAddressSqlOperatorConversions.StringifyOperatorConversion.class,
      IpAddressSqlOperatorConversions.PrefixOperatorConversion.class,
      IpAddressSqlOperatorConversions.MatchOperatorConversion.class,
      IpAddressSqlOperatorConversions.SearchOperatorConversion.class,
      IpAddressSqlOperatorConversions.HostOperatorConversion.class
  );

  @Mock
  private ExprMacroTable.ExprMacro exprMacro;
  @Mock
  private SqlOperatorConversion sqlOperatorConversion;

  private Injector injector;
  private IpAddressModule target;

  @Before
  public void setup()
  {
    target = new IpAddressModule();
  }


  @Test
  public void testExpressionsShouldBeBound()
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
    Assert.assertEquals(IP_ADDRESS_EXPRESSIONS, Sets.intersection(IP_ADDRESS_EXPRESSIONS, expressionMacroClasses));
  }

  @Test
  public void testSqlOperatorConversionsShouldBeBound()
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
    Assert.assertEquals(IP_ADDRESS_SQL_OPERATORS, Sets.intersection(IP_ADDRESS_SQL_OPERATORS, sqlOperatorClasses));
  }

  private Injector createInjector()
  {
    return Guice.createInjector(
        target,
        new ExpressionModule(),
        new JacksonModule(),
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
