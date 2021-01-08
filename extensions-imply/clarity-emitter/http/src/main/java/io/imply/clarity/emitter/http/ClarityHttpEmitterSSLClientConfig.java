/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.clarity.emitter.http;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.druid.metadata.PasswordProvider;

public class ClarityHttpEmitterSSLClientConfig
{
  @JsonProperty
  private String protocol;

  @JsonProperty
  private String trustStoreType;

  @JsonProperty
  private String trustStorePath;

  @JsonProperty
  private String trustStoreAlgorithm;

  @JsonProperty("trustStorePassword")
  private PasswordProvider trustStorePasswordProvider;

  public String getProtocol()
  {
    return protocol;
  }

  public String getTrustStoreType()
  {
    return trustStoreType;
  }

  public String getTrustStorePath()
  {
    return trustStorePath;
  }

  public String getTrustStoreAlgorithm()
  {
    return trustStoreAlgorithm;
  }

  public PasswordProvider getTrustStorePasswordProvider()
  {
    return trustStorePasswordProvider;
  }

  @Override
  public String toString()
  {
    return "ClarityHttpEmitterSSLClientConfig{" +
           "protocol='" + protocol + '\'' +
           ", trustStoreType='" + trustStoreType + '\'' +
           ", trustStorePath='" + trustStorePath + '\'' +
           ", trustStoreAlgorithm='" + trustStoreAlgorithm + '\'' +
           '}';
  }
}
