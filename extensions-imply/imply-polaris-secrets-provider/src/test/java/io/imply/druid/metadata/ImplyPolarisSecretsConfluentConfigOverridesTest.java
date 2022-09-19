/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.metadata;

import com.amazonaws.services.secretsmanager.AWSSecretsManager;
import com.amazonaws.services.secretsmanager.model.GetSecretValueResult;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import io.imply.druid.metadata.secrets.ConfluentInputConnectionSecrets;
import org.apache.druid.java.util.common.StringUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.Map;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;


public class ImplyPolarisSecretsConfluentConfigOverridesTest
{
  private static final ObjectMapper OBJECT_MAPPER =
      new ObjectMapper()
          .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

  ImplyPolarisSecretsConfluentConfigOverrides configOverrides;
  AWSSecretsManager secretsManager = Mockito.mock(AWSSecretsManager.class);

  @Before
  public void setup()
  {
    configOverrides = new ImplyPolarisSecretsConfluentConfigOverrides(
        ImplyPolarisSecretsConfluentConfigOverrides.CONFLUENT_SECRETS_TYPE,
        "testSecretName",
        secretsManager
    );
  }

  @Test
  public void testOverrideConfigs() throws Exception
  {
    GetSecretValueResult getSecretValueResult = new GetSecretValueResult();
    getSecretValueResult.setName("testSecretName");
    getSecretValueResult.setSecretString(
        OBJECT_MAPPER.writeValueAsString(
            new ConfluentInputConnectionSecrets("testKey", "testValue")
        )
    );
    when(secretsManager.getSecretValue(any())).thenReturn(getSecretValueResult);

    Map<String, Object> originalMap = ImmutableMap.of("hello", "world");
    Map<String, Object> newMap = configOverrides.overrideConfigs(originalMap);

    Assert.assertEquals(
        StringUtils.format(
            ImplyPolarisSecretsConfluentConfigOverrides.SASL_JAAS_CONFIG_FORMAT,
            "testKey",
            "testValue"
        ),
        newMap.get(ImplyPolarisSecretsConfluentConfigOverrides.SASL_JAAS_CONFIG_PROPERTY)
    );
    Assert.assertEquals("world", newMap.get("hello"));
  }
}
