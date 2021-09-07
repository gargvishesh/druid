/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package org.apache.druid.query.aggregation.datasketches.tuple;

import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.jsontype.NamedType;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.google.common.collect.ImmutableList;
import com.google.inject.Binder;
import com.google.inject.Inject;
import org.apache.druid.initialization.DruidModule;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.license.ImplyLicenseManager;

import java.util.Collections;
import java.util.List;

public class ArrayOfDoublesSketchAdTechModule implements DruidModule
{
  static final String AD_TECH_INVENTORY = "adTechInventory";
  private ImplyLicenseManager implyLicenseManager;
  private final Logger log = new Logger(ArrayOfDoublesSketchAdTechModule.class);

  @Inject
  public void setImplyLicenseManager(ImplyLicenseManager implyLicenseManager)
  {
    this.implyLicenseManager = implyLicenseManager;
  }

  @Override
  public void configure(Binder binder)
  {

  }

  @Override
  public List<? extends Module> getJacksonModules()
  {
    if (!implyLicenseManager.isFeatureEnabled("ad-tech-aggregators")) {
      return ImmutableList.of();
    }

    log.info("The ad-tech-aggregators feature is enabled");
    return Collections.<Module>singletonList(
        new SimpleModule("ArrayOfDoublesSketchAdTechModule").registerSubtypes(
            new NamedType(
                AdTechInventoryAggregatorFactory.class,
                AD_TECH_INVENTORY
            )));
  }
}
