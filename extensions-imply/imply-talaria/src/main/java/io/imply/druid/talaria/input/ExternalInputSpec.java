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
import com.google.common.base.Preconditions;
import org.apache.druid.data.input.InputFormat;
import org.apache.druid.data.input.InputSource;
import org.apache.druid.segment.column.RowSignature;

import java.util.Objects;

@JsonTypeName("external")
public class ExternalInputSpec implements InputSpec
{
  private final InputSource inputSource;
  private final InputFormat inputFormat;
  private final RowSignature signature;

  @JsonCreator
  public ExternalInputSpec(
      @JsonProperty("inputSource") InputSource inputSource,
      @JsonProperty("inputFormat") InputFormat inputFormat,
      @JsonProperty("signature") RowSignature signature
  )
  {
    this.inputSource = Preconditions.checkNotNull(inputSource, "inputSource");
    this.inputFormat = Preconditions.checkNotNull(inputFormat, "inputFormat");
    this.signature = Preconditions.checkNotNull(signature, "signature");
  }

  @JsonProperty
  public InputSource getInputSource()
  {
    return inputSource;
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
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    ExternalInputSpec that = (ExternalInputSpec) o;
    return Objects.equals(inputSource, that.inputSource)
           && Objects.equals(inputFormat, that.inputFormat)
           && Objects.equals(signature, that.signature);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(inputSource, inputFormat, signature);
  }

  @Override
  public String toString()
  {
    return "ExternalInputSpec{" +
           "inputSources=" + inputSource +
           ", inputFormat=" + inputFormat +
           ", signature=" + signature +
           '}';
  }
}
