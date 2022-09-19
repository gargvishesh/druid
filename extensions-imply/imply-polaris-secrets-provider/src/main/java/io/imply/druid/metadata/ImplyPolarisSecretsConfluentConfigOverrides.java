/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.metadata;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.services.secretsmanager.AWSSecretsManager;
import com.amazonaws.services.secretsmanager.AWSSecretsManagerClient;
import com.amazonaws.services.secretsmanager.AWSSecretsManagerClientBuilder;
import com.amazonaws.services.secretsmanager.model.GetSecretValueRequest;
import com.amazonaws.services.secretsmanager.model.GetSecretValueResult;
import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.imply.druid.metadata.secrets.ConfluentInputConnectionSecrets;
import org.apache.druid.indexing.seekablestream.extension.KafkaConfigOverrides;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.logger.Logger;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

@JsonTypeName("polarisSecretsConfluent")
public class ImplyPolarisSecretsConfluentConfigOverrides implements KafkaConfigOverrides
{
  public static final String SASL_JAAS_CONFIG_PROPERTY = "sasl.jaas.config";
  public static final String SASL_JAAS_CONFIG_FORMAT =
      "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"%s\" password=\"%s\";";
  public static final String CONFLUENT_SECRETS_TYPE = "confluent";

  private static final Logger log = new Logger(ImplyPolarisSecretsConfluentConfigOverrides.class);
  private static final ObjectMapper OBJECT_MAPPER =
      new ObjectMapper()
          .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

  private final String secretType;
  private final String secretKey;
  private final AWSSecretsManager secretsManagerClient;

  @JsonCreator
  public static ImplyPolarisSecretsConfluentConfigOverrides jsonCreator(
      @JacksonInject ImplyPolarisSecretsProviderConfig providerConfig,
      @JsonProperty("secretType") String secretType,
      @JsonProperty("secretKey") String secretKey
  )
  {
    final AWSSecretsManagerClientBuilder builder = AWSSecretsManagerClient.builder();

    if (providerConfig.getRegion() != null) {
      builder.withRegion(providerConfig.getRegion());
    }

    if (providerConfig.getTestingAccessKey() != null
        && providerConfig.getTestingSecretKey() != null) {
      builder.withCredentials(
          new AWSStaticCredentialsProvider(
              new AWSCredentials()
              {
                @Override
                public String getAWSAccessKeyId()
                {
                  return providerConfig.getTestingAccessKey();
                }

                @Override
                public String getAWSSecretKey()
                {
                  return providerConfig.getTestingSecretKey();
                }
              }
          )
      );
    }

    return new ImplyPolarisSecretsConfluentConfigOverrides(
        secretType,
        secretKey,
        builder.build()
    );
  }

  public ImplyPolarisSecretsConfluentConfigOverrides(
      String secretType,
      String secretKey,
      AWSSecretsManager secretsManagerClient
  )
  {
    this.secretType = secretType;
    this.secretKey = secretKey;
    this.secretsManagerClient = secretsManagerClient;
  }

  @Override
  public Map<String, Object> overrideConfigs(Map<String, Object> originalConsumerProperties)
  {
    final GetSecretValueRequest getSecretValueRequest = new GetSecretValueRequest();
    log.debug("Fetching secret with ID: " + secretKey);
    getSecretValueRequest.setSecretId(secretKey);
    GetSecretValueResult secretValueResult = secretsManagerClient.getSecretValue(getSecretValueRequest);
    final String secretJson = secretValueResult.getSecretString();

    final Map<String, Object> overrideMap = new HashMap<>(originalConsumerProperties);
    updateConfigMap(overrideMap, secretType, secretJson);
    return overrideMap;
  }

  @JsonProperty
  public String getSecretType()
  {
    return secretType;
  }

  @JsonProperty
  public String getSecretKey()
  {
    return secretKey;
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
    ImplyPolarisSecretsConfluentConfigOverrides that = (ImplyPolarisSecretsConfluentConfigOverrides) o;
    return Objects.equals(secretType, that.secretType) && Objects.equals(secretKey, that.secretKey);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(secretType, secretKey);
  }

  @Override
  public String toString()
  {
    return "ImplyPolarisSecretsKafkaConfigOverrides{" +
           "secretType='" + secretType + '\'' +
           ", secretKey='" + secretKey + '\'' +
           '}';
  }

  private void updateConfigMap(final Map<String, Object> configMap, final String secretType, final String secretJson)
  {
    try {
      if (CONFLUENT_SECRETS_TYPE.equals(secretType)) {
        ConfluentInputConnectionSecrets inputConnectionSecrets = OBJECT_MAPPER.readValue(
            secretJson,
            ConfluentInputConnectionSecrets.class
        );

        configMap.put(
            SASL_JAAS_CONFIG_PROPERTY,
            StringUtils.format(
                SASL_JAAS_CONFIG_FORMAT,
                inputConnectionSecrets.getKey(),
                inputConnectionSecrets.getSecret()
            )
        );
      } else {
        throw new RuntimeException("Unrecognized secret type: " + secretType);
      }
    }
    catch (Exception e) {
      log.error("Could not deserialize secrets. Type: " + secretType);
      throw new RuntimeException("Could not get secret for Imply Polaris.");
    }
  }
}
