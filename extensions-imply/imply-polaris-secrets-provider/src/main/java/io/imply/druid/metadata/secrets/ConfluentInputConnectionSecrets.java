/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.metadata.secrets;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonPOJOBuilder;

import javax.validation.Valid;
import java.util.Objects;

@JsonDeserialize(builder = ConfluentInputConnectionSecrets.Builder.class)
public class ConfluentInputConnectionSecrets
{

  @JsonInclude(value = JsonInclude.Include.NON_NULL)
  private final @Valid
  String key;

  @JsonInclude(value = JsonInclude.Include.NON_NULL)
  private final @Valid
  String secret;

  public ConfluentInputConnectionSecrets(String key, String secret)
  {
    this.key = key;
    this.secret = secret;
  }

  public static Builder builder()
  {
    return new Builder();
  }

  /**
   * Return a new builder pre-populated with this instance's data.
   */
  public Builder with()
  {
    return (new Builder())
        .secret(this.getSecret())
        .key(this.getKey());
  }


  @JsonProperty("key")
  public String getKey()
  {
    return key;
  }

  @JsonProperty("secret")
  public String getSecret()
  {
    return secret;
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
    ConfluentInputConnectionSecrets that = (ConfluentInputConnectionSecrets) o;
    return Objects.equals(key, that.key) && Objects.equals(secret, that.secret);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(key, secret);
  }

  @JsonPOJOBuilder
  public static class Builder
  {
    private @Valid String key;
    private @Valid String secret;

    /**
     *
     */

    @JsonProperty("key")
    public Builder key(final @Valid String key)
    {
      this.key = key;
      return this;
    }

    @JsonProperty("secret")
    public Builder secret(final @Valid String secret)
    {
      this.secret = secret;
      return this;
    }



    public ConfluentInputConnectionSecrets build()
    {
      return new ConfluentInputConnectionSecrets(key, secret);
    }
  }
}

