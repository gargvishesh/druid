/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.sql.calcite.external;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.druid.data.input.InputFormat;
import org.apache.druid.data.input.InputSource;
import org.apache.druid.segment.column.RowSignature;

import javax.annotation.Nullable;
import javax.validation.constraints.NotNull;
import java.util.Objects;

/**
 * Catalog form of a Polaris external table specification used to pass along the three
 * components needed for an external table in MSQ ingest.
 */
public class PolarisExternalTableSpec
{
  @NotNull private final InputSource inputSource;
  @Nullable private final InputFormat inputFormat;
  @Nullable private final RowSignature signature;

  @JsonCreator
  public PolarisExternalTableSpec(
      @NotNull @JsonProperty("inputSource") final InputSource inputSource,
      @Nullable @JsonProperty("inputFormat") final InputFormat inputFormat,
      @Nullable @JsonProperty("signature") final RowSignature signature)
  {
    this.inputSource = inputSource;
    this.inputFormat = inputFormat;
    this.signature = signature;
  }

  @NotNull
  @JsonProperty
  InputSource getInputSource()
  {
    return inputSource;
  }

  @Nullable
  @JsonProperty
  InputFormat getInputFormat()
  {
    return inputFormat;
  }

  @Nullable
  @JsonProperty
  RowSignature getSignature()
  {
    return signature;
  }

  @Override
  public String toString()
  {
    return "PolarisExternalTableSpec{" +
           "inputSource='" + inputSource + '\'' +
           ", inputFormat=" + inputFormat +
           ", signature=" + signature +
           '}';
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
    PolarisExternalTableSpec that = (PolarisExternalTableSpec) o;
    return Objects.equals(inputSource, that.inputSource)
           && Objects.equals(inputFormat, that.inputFormat)
           && Objects.equals(signature, that.signature);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(inputSource, inputFormat, signature);
  }
}
