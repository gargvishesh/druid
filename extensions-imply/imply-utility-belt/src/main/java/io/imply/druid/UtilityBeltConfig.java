/*
 * Copyright (c) 2019 Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 *  of Imply Data, Inc.
 */

package io.imply.druid;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.io.File;

public class UtilityBeltConfig
{
  @JsonProperty
  public File geoDatabase = null;

  public File getGeoDatabaseFile()
  {
    return geoDatabase;
  }
}
