/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.talaria.indexing;

import com.fasterxml.jackson.annotation.JsonCreator;

public class ExternalTalariaDestination implements TalariaDestination
{
  public static final ExternalTalariaDestination INSTANCE = new ExternalTalariaDestination();
  static final String TYPE = "external";

  private ExternalTalariaDestination()
  {
    // Singleton.
  }

  @JsonCreator
  public static ExternalTalariaDestination instance()
  {
    return INSTANCE;
  }

  @Override
  public String toString()
  {
    return "ExternalTalariaDestination{}";
  }
}
