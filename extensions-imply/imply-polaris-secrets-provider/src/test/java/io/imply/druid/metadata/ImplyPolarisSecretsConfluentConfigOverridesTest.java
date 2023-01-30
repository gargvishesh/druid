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
import io.imply.druid.metadata.secrets.KafkaInputConnectionSecrets;
import nl.jqno.equalsverifier.EqualsVerifier;
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

  ImplyPolarisSecretsConfluentConfigOverrides confluentOverrides;
  ImplyPolarisSecretsConfluentConfigOverrides kafkaOverrides;
  AWSSecretsManager secretsManager = Mockito.mock(AWSSecretsManager.class);

  @Before
  public void setup()
  {
    confluentOverrides = new ImplyPolarisSecretsConfluentConfigOverrides(
        ImplyPolarisSecretsConfluentConfigOverrides.CONFLUENT_SECRETS_TYPE,
        "testSecretName",
        secretsManager
    );
    kafkaOverrides = new ImplyPolarisSecretsConfluentConfigOverrides(
        ImplyPolarisSecretsConfluentConfigOverrides.KAFKA_SECRETS_TYPE,
        "anotherTestSecretName",
        secretsManager
    );
  }

  @Test
  public void testConfluentOverrideConfigs() throws Exception
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
    Map<String, Object> newMap = confluentOverrides.overrideConfigs(originalMap);

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

  @Test
  public void testKafkaOverrideConfigs() throws Exception
  {
    GetSecretValueResult getSecretValueResult = new GetSecretValueResult();
    getSecretValueResult.setName("anotherTestSecretName");
    getSecretValueResult.setSecretString(
        OBJECT_MAPPER.writeValueAsString(
            new KafkaInputConnectionSecrets(ImmutableMap.of("sasl.mechanism", "SCRAM-SHA-256"))
        )
    );
    when(secretsManager.getSecretValue(any())).thenReturn(getSecretValueResult);

    Map<String, Object> originalMap = ImmutableMap.of(
        "hello", "world",
        "sasl.mechanism", "some-default-value"
    );
    Map<String, Object> newMap = kafkaOverrides.overrideConfigs(originalMap);

    Assert.assertEquals("SCRAM-SHA-256", newMap.get("sasl.mechanism"));
    Assert.assertEquals("world", newMap.get("hello"));
  }

  @Test
  public void testEquals()
  {
    EqualsVerifier.forClass(ConfluentInputConnectionSecrets.class)
                  .usingGetClass()
                  .verify();
    EqualsVerifier.forClass(KafkaInputConnectionSecrets.class)
                  .usingGetClass()
                  .verify();
  }
}
