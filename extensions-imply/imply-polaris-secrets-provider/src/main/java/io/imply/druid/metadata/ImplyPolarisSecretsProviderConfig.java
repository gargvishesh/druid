/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.metadata;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.Nullable;
import java.util.Objects;

public class ImplyPolarisSecretsProviderConfig
{
  @JsonProperty
  @Nullable
  private final String region;

  @JsonProperty
  @Nullable
  private final String testingAccessKey;

  @JsonProperty
  @Nullable
  private final String testingSecretKey;

  @JsonCreator
  public ImplyPolarisSecretsProviderConfig(
      @JsonProperty("region") @Nullable String region,
      @JsonProperty("testingAccessKey") @Nullable String testingAccessKey,
      @JsonProperty("testingSecretKey") @Nullable String testingSecretKey
  )
  {
    this.region = region;
    this.testingAccessKey = testingAccessKey;
    this.testingSecretKey = testingSecretKey;
  }

  @JsonProperty
  @Nullable
  public String getRegion()
  {
    return region;
  }

  @JsonProperty
  @Nullable
  public String getTestingAccessKey()
  {
    return testingAccessKey;
  }

  @JsonProperty
  @Nullable
  public String getTestingSecretKey()
  {
    return testingSecretKey;
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    ImplyPolarisSecretsProviderConfig that = (ImplyPolarisSecretsProviderConfig) o;
    return Objects.equals(region, that.region) && Objects.equals(
        testingAccessKey,
        that.testingAccessKey
    ) && Objects.equals(testingSecretKey, that.testingSecretKey);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(region, testingAccessKey, testingSecretKey);
  }
}
