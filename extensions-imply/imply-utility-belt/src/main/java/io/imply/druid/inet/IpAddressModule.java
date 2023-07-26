/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.inet;

import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.jsontype.NamedType;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.google.common.annotations.VisibleForTesting;
import com.google.inject.Binder;
import io.imply.druid.inet.column.IpAddressBlob;
import io.imply.druid.inet.column.IpAddressBlobJsonSerializer;
import io.imply.druid.inet.column.IpAddressComplexTypeSerde;
import io.imply.druid.inet.column.IpAddressDimensionHandler;
import io.imply.druid.inet.column.IpAddressDimensionSchema;
import io.imply.druid.inet.column.IpPrefixBlob;
import io.imply.druid.inet.column.IpPrefixBlobJsonSerializer;
import io.imply.druid.inet.column.IpPrefixComplexTypeSerde;
import io.imply.druid.inet.column.IpPrefixDimensionHandler;
import io.imply.druid.inet.column.IpPrefixDimensionSchema;
import io.imply.druid.inet.expression.IpAddressExpressions;
import io.imply.druid.inet.segment.virtual.IpAddressFormatVirtualColumn;
import io.imply.druid.inet.sql.IpAddressSqlOperatorConversions;
import org.apache.druid.guice.ExpressionModule;
import org.apache.druid.initialization.DruidModule;
import org.apache.druid.segment.DimensionHandlerUtils;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.serde.ComplexMetrics;
import org.apache.druid.sql.guice.SqlBindings;

import java.util.Collections;
import java.util.List;

public class IpAddressModule implements DruidModule
{
  public static final String ADDRESS_TYPE_NAME = "ipAddress";
  public static final ColumnType ADDRESS_TYPE = ColumnType.ofComplex(IpAddressModule.ADDRESS_TYPE_NAME);
  public static final String PREFIX_TYPE_NAME = "ipPrefix";
  public static final ColumnType PREFIX_TYPE = ColumnType.ofComplex(IpAddressModule.PREFIX_TYPE_NAME);

  @Override
  public List<? extends Module> getJacksonModules()
  {
    return Collections.singletonList(
        new SimpleModule("IpAddressModule")
            .registerSubtypes(
                new NamedType(IpAddressDimensionSchema.class, ADDRESS_TYPE_NAME),
                new NamedType(IpPrefixDimensionSchema.class, PREFIX_TYPE_NAME),
                new NamedType(IpAddressFormatVirtualColumn.class, IpAddressFormatVirtualColumn.TYPE_NAME)
            )
            .addSerializer(IpAddressBlob.class, new IpAddressBlobJsonSerializer())
            .addSerializer(IpPrefixBlob.class, new IpPrefixBlobJsonSerializer())
    );
  }

  @Override
  public void configure(Binder binder)
  {
    registerHandlersAndSerde();
    ExpressionModule.addExprMacro(binder, IpAddressExpressions.AddressParseExprMacro.class);
    ExpressionModule.addExprMacro(binder, IpAddressExpressions.PrefixParseExprMacro.class);
    ExpressionModule.addExprMacro(binder, IpAddressExpressions.AddressTryParseExprMacro.class);
    ExpressionModule.addExprMacro(binder, IpAddressExpressions.PrefixTryParseExprMacro.class);
    ExpressionModule.addExprMacro(binder, IpAddressExpressions.CompareExprMacro.class);
    ExpressionModule.addExprMacro(binder, IpAddressExpressions.StringifyExprMacro.class);
    ExpressionModule.addExprMacro(binder, IpAddressExpressions.PrefixExprMacro.class);
    ExpressionModule.addExprMacro(binder, IpAddressExpressions.MatchExprMacro.class);
    ExpressionModule.addExprMacro(binder, IpAddressExpressions.SearchExprMacro.class);
    ExpressionModule.addExprMacro(binder, IpAddressExpressions.HostExprMacro.class);

    SqlBindings.addOperatorConversion(binder, IpAddressSqlOperatorConversions.AddressParseOperatorConversion.class);
    SqlBindings.addOperatorConversion(binder, IpAddressSqlOperatorConversions.PrefixParseOperatorConversion.class);
    SqlBindings.addOperatorConversion(binder, IpAddressSqlOperatorConversions.AddressTryParseOperatorConversion.class);
    SqlBindings.addOperatorConversion(binder, IpAddressSqlOperatorConversions.PrefixTryParseOperatorConversion.class);
    SqlBindings.addOperatorConversion(binder, IpAddressSqlOperatorConversions.CompareOperatorConversion.class);
    SqlBindings.addOperatorConversion(binder, IpAddressSqlOperatorConversions.StringifyOperatorConversion.class);
    SqlBindings.addOperatorConversion(binder, IpAddressSqlOperatorConversions.PrefixOperatorConversion.class);
    SqlBindings.addOperatorConversion(binder, IpAddressSqlOperatorConversions.MatchOperatorConversion.class);
    SqlBindings.addOperatorConversion(binder, IpAddressSqlOperatorConversions.SearchOperatorConversion.class);
    SqlBindings.addOperatorConversion(binder, IpAddressSqlOperatorConversions.HostOperatorConversion.class);
  }

  @VisibleForTesting
  public static void registerHandlersAndSerde()
  {
    if (ComplexMetrics.getSerdeForType(ADDRESS_TYPE_NAME) == null) {
      ComplexMetrics.registerSerde(ADDRESS_TYPE_NAME, IpAddressComplexTypeSerde.INSTANCE);
    }

    DimensionHandlerUtils.registerDimensionHandlerProvider(
        ADDRESS_TYPE_NAME,
        IpAddressDimensionHandler::new
    );

    if (ComplexMetrics.getSerdeForType(PREFIX_TYPE_NAME) == null) {
      ComplexMetrics.registerSerde(PREFIX_TYPE_NAME, IpPrefixComplexTypeSerde.INSTANCE);
    }

    DimensionHandlerUtils.registerDimensionHandlerProvider(
        PREFIX_TYPE_NAME,
        IpPrefixDimensionHandler::new
    );
  }
}
