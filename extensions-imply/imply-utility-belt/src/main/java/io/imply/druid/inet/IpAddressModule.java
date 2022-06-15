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
import com.google.inject.Inject;
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
import io.imply.druid.license.ImplyLicenseManager;
import org.apache.druid.guice.ExpressionModule;
import org.apache.druid.initialization.DruidModule;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.segment.DimensionHandlerUtils;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.serde.ComplexMetrics;
import org.apache.druid.sql.guice.SqlBindings;

import java.util.Collections;
import java.util.List;

public class IpAddressModule implements DruidModule
{
  private static final Logger log = new Logger(IpAddressModule.class);

  @VisibleForTesting
  public static final String FEATURE_NAME = "enhanced-ip-support";
  public static final String ADDRESS_TYPE_NAME = "ipAddress";
  public static final ColumnType ADDRESS_TYPE = ColumnType.ofComplex(IpAddressModule.ADDRESS_TYPE_NAME);
  public static final String PREFIX_TYPE_NAME = "ipPrefix";
  public static final ColumnType PREFIX_TYPE = ColumnType.ofComplex(IpAddressModule.PREFIX_TYPE_NAME);
  private ImplyLicenseManager implyLicenseManager;

  @Inject
  public void setImplyLicenseManager(ImplyLicenseManager implyLicenseManager)
  {
    this.implyLicenseManager = implyLicenseManager;
  }

  @Override
  public List<? extends Module> getJacksonModules()
  {
    if (!implyLicenseManager.isFeatureEnabled(FEATURE_NAME)) {
      log.info(FEATURE_NAME + " is not enabled. Not binding jackson modules.");
      return Collections.emptyList();
    }

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
    if (!implyLicenseManager.isFeatureEnabled(FEATURE_NAME)) {
      log.info(FEATURE_NAME + " is not enabled. Not binding operators.");
      return;
    }
    registerHandlersAndSerde();
    ExpressionModule.addExprMacro(binder, IpAddressExpressions.ParseExprMacro.class);
    ExpressionModule.addExprMacro(binder, IpAddressExpressions.TryParseExprMacro.class);
    ExpressionModule.addExprMacro(binder, IpAddressExpressions.StringifyExprMacro.class);
    ExpressionModule.addExprMacro(binder, IpAddressExpressions.PrefixExprMacro.class);
    ExpressionModule.addExprMacro(binder, IpAddressExpressions.MatchExprMacro.class);

    SqlBindings.addOperatorConversion(binder, IpAddressSqlOperatorConversions.ParseOperatorConversion.class);
    SqlBindings.addOperatorConversion(binder, IpAddressSqlOperatorConversions.TryParseOperatorConversion.class);
    SqlBindings.addOperatorConversion(binder, IpAddressSqlOperatorConversions.StringifyOperatorConversion.class);
    SqlBindings.addOperatorConversion(binder, IpAddressSqlOperatorConversions.PrefixOperatorConversion.class);
    SqlBindings.addOperatorConversion(binder, IpAddressSqlOperatorConversions.MatchOperatorConversion.class);
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
