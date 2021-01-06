/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.ingest.config;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class IngestServiceTenantConfig
{
  private String accountId;
  private String clusterId;

  @JsonCreator
  public IngestServiceTenantConfig(
      @JsonProperty("accountId") String accountId,
      @JsonProperty("clusterId") String clusterId
  )
  {
    this.accountId = accountId;
    this.clusterId = clusterId;
  }

  @JsonProperty
  public String getAccountId()
  {
    return accountId;
  }

  @JsonProperty
  public String getClusterId()
  {
    return clusterId;
  }
}
