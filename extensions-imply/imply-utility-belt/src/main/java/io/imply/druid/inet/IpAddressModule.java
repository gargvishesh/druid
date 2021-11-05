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
import io.imply.druid.inet.column.IpAddressComplexTypeSerde;
import io.imply.druid.inet.column.IpAddressDimensionHandler;
import io.imply.druid.inet.column.IpAddressDimensionSchema;
import org.apache.druid.initialization.DruidModule;
import org.apache.druid.segment.DimensionHandlerUtils;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.serde.ComplexMetrics;

import java.util.Collections;
import java.util.List;

public class IpAddressModule implements DruidModule
{
  public static final String TYPE_NAME = "ipAddress";
  public static final ColumnType TYPE = ColumnType.ofComplex(IpAddressModule.TYPE_NAME);

  @Override
  public List<? extends Module> getJacksonModules()
  {
    return Collections.singletonList(
        new SimpleModule("IpAddressModule")
            .registerSubtypes(
                new NamedType(IpAddressDimensionSchema.class, TYPE_NAME)
            )
    );
  }

  @Override
  public void configure(Binder binder)
  {
    registerHandlersAndSerde();
  }

  @VisibleForTesting
  public static void registerHandlersAndSerde()
  {
    if (ComplexMetrics.getSerdeForType(TYPE_NAME) == null) {
      ComplexMetrics.registerSerde(TYPE_NAME, IpAddressComplexTypeSerde.INSTANCE);
    }

    DimensionHandlerUtils.registerDimensionHandlerProvider(
        TYPE_NAME,
        IpAddressDimensionHandler::new
    );
  }
}
