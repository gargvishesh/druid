/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.talaria.input;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import org.apache.druid.data.input.InputFormat;
import org.apache.druid.data.input.InputSource;
import org.apache.druid.segment.column.RowSignature;

import java.util.List;
import java.util.Objects;

/**
 * Input slice representing external data.
 *
 * Corresponds to {@link org.apache.druid.sql.calcite.external.ExternalDataSource}.
 */
@JsonTypeName("external")
public class ExternalInputSlice implements InputSlice
{
  private final List<InputSource> inputSources;
  private final InputFormat inputFormat;
  private final RowSignature signature;

  @JsonCreator
  public ExternalInputSlice(
      @JsonProperty("inputSources") List<InputSource> inputSources,
      @JsonProperty("inputFormat") InputFormat inputFormat,
      @JsonProperty("signature") RowSignature signature
  )
  {
    this.inputSources = inputSources;
    this.inputFormat = inputFormat;
    this.signature = signature;
  }

  @JsonProperty
  public List<InputSource> getInputSources()
  {
    return inputSources;
  }

  @JsonProperty
  public InputFormat getInputFormat()
  {
    return inputFormat;
  }

  @JsonProperty
  public RowSignature getSignature()
  {
    return signature;
  }

  @Override
  public int numFiles()
  {
    return inputSources.size();
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
    ExternalInputSlice that = (ExternalInputSlice) o;
    return Objects.equals(inputSources, that.inputSources)
           && Objects.equals(inputFormat, that.inputFormat)
           && Objects.equals(signature, that.signature);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(inputSources, inputFormat, signature);
  }

  @Override
  public String toString()
  {
    return "ExternalInputSlice{" +
           "inputSources=" + inputSources +
           ", inputFormat=" + inputFormat +
           ", signature=" + signature +
           '}';
  }
}
