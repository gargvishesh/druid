/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.ingest.config;

import com.fasterxml.jackson.annotation.JsonProperty;

public class IngestServiceTenantConfig
{
  @JsonProperty("accountId")
  private String accountId;

  @JsonProperty("clusterId")
  private String clusterId;

  public String getAccountId()
  {
    return accountId;
  }

  public String getClusterId()
  {
    return clusterId;
  }
}
