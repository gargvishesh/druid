/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.nested;

import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.jsontype.NamedType;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.google.common.annotations.VisibleForTesting;
import com.google.inject.Binder;
import io.imply.druid.nested.column.JsonDimensionHandler;
import io.imply.druid.nested.column.NestedDataComplexTypeSerde;
import io.imply.druid.nested.column.NestedDataDimensionHandler;
import io.imply.druid.nested.column.NestedDataDimensionSchema;
import io.imply.druid.nested.column.StructuredData;
import io.imply.druid.nested.column.StructuredDataJsonSerializer;
import io.imply.druid.nested.expressions.NestedDataExpressions;
import io.imply.druid.nested.sql.NestedDataOperatorConversions;
import io.imply.druid.nested.virtual.NestedFieldVirtualColumn;
import org.apache.druid.guice.ExpressionModule;
import org.apache.druid.initialization.DruidModule;
import org.apache.druid.segment.DimensionHandlerUtils;
import org.apache.druid.segment.serde.ComplexMetrics;
import org.apache.druid.sql.guice.SqlBindings;

import java.util.Collections;
import java.util.List;

public class NestedDataModule implements DruidModule
{
  @Override
  public List<? extends Module> getJacksonModules()
  {
    return getJacksonModulesList();
  }

  @Override
  public void configure(Binder binder)
  {
    registerHandlersAndSerde();
    registerExpressionAndSqlOperators(binder);
  }


  @VisibleForTesting
  public static void registerHandlersAndSerde()
  {
    if (ComplexMetrics.getSerdeForType(NestedDataComplexTypeSerde.TYPE_NAME) == null) {
      ComplexMetrics.registerSerde(NestedDataComplexTypeSerde.TYPE_NAME, NestedDataComplexTypeSerde.INSTANCE);
    }
    DimensionHandlerUtils.registerDimensionHandlerProvider(
        NestedDataComplexTypeSerde.TYPE_NAME,
        JsonDimensionHandler::new
    );

    if (ComplexMetrics.getSerdeForType(NestedDataComplexTypeSerde.GENERIC_TYPE_NAME) == null) {
      ComplexMetrics.registerSerde(NestedDataComplexTypeSerde.GENERIC_TYPE_NAME, NestedDataComplexTypeSerde.INSTANCE);
    }
    DimensionHandlerUtils.registerDimensionHandlerProvider(
        NestedDataComplexTypeSerde.GENERIC_TYPE_NAME,
        NestedDataDimensionHandler::new
    );
  }

  public static void registerExpressionAndSqlOperators(Binder binder)
  {
    ExpressionModule.addExprMacro(binder, NestedDataExpressions.StructExprMacro.class);
    ExpressionModule.addExprMacro(binder, NestedDataExpressions.GetPathExprMacro.class);

    SqlBindings.addOperatorConversion(binder, NestedDataOperatorConversions.GetPathOperatorConversion.class);
    SqlBindings.addOperatorConversion(binder, NestedDataOperatorConversions.JsonGetPathAliasOperatorConversion.class);
  }


  public static List<SimpleModule> getJacksonModulesList()
  {
    return Collections.singletonList(
        new SimpleModule("NestedDataModule")
            .registerSubtypes(
                new NamedType(NestedDataDimensionSchema.class, NestedDataComplexTypeSerde.TYPE_NAME),
                new NamedType(NestedDataDimensionSchema.class, NestedDataComplexTypeSerde.GENERIC_TYPE_NAME),
                new NamedType(NestedFieldVirtualColumn.class, "nested-field")
            )
            .addSerializer(StructuredData.class, new StructuredDataJsonSerializer())
    );
  }
}
