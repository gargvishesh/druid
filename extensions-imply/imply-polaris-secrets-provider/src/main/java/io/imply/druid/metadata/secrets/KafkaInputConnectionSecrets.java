/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.metadata.secrets;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Map;
import java.util.Objects;

public class KafkaInputConnectionSecrets
{
  private final Map<String, Object> kafkaConsumerProperties;

  @JsonCreator
  public KafkaInputConnectionSecrets(
      @JsonProperty("kafkaConsumerProperties") Map<String, Object> kafkaConsumerProperties
  )
  {
    this.kafkaConsumerProperties = kafkaConsumerProperties;
  }

  public Map<String, Object> getKafkaConsumerProperties()
  {
    return kafkaConsumerProperties;
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
    KafkaInputConnectionSecrets that = (KafkaInputConnectionSecrets) o;
    return Objects.equals(kafkaConsumerProperties, that.kafkaConsumerProperties);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(kafkaConsumerProperties);
  }
}
