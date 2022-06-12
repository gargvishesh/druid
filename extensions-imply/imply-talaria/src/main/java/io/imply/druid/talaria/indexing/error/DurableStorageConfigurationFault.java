/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.talaria.indexing.error;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import io.imply.druid.talaria.guice.TalariaIndexingModule;
import io.imply.druid.talaria.util.TalariaContext;

import java.util.Objects;

@JsonTypeName(DurableStorageConfigurationFault.CODE)
public class DurableStorageConfigurationFault extends BaseTalariaFault
{
  static final String CODE = "DurableStorageConfiguration";

  private final String errorMessage;

  @JsonCreator
  public DurableStorageConfigurationFault(@JsonProperty("message") final String errorMessage)
  {
    super(
        CODE,
        "Durable storage mode can only be enabled when %s is set to true and "
        + "the connector is configured correctly. "
        + "Check the documentation on how to enable durable storage mode. "
        + "If you want to still query without durable storage mode, set %s to false in the query context. Got error %s",
        TalariaIndexingModule.TALARIA_INTERMEDIATE_STORAGE_ENABLED,
        TalariaContext.CTX_DURABLE_SHUFFLE_STORAGE,
        errorMessage
    );
    this.errorMessage = errorMessage;
  }

  @JsonProperty
  public String getMessage()
  {
    return errorMessage;
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
    if (!super.equals(o)) {
      return false;
    }
    DurableStorageConfigurationFault that = (DurableStorageConfigurationFault) o;
    return Objects.equals(errorMessage, that.errorMessage);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(super.hashCode(), errorMessage);
  }
}
