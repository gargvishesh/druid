package io.imply.druid.sql.calcite.functions;

import org.apache.druid.data.input.InputFormat;
import org.apache.druid.data.input.InputSource;
import org.apache.druid.segment.column.RowSignature;

import javax.annotation.Nullable;
import javax.validation.constraints.NotNull;

public class PolarisExternalTableSpec
{
  @NotNull private final InputSource inputSource;
  @Nullable private final InputFormat inputFormat;
  @Nullable private final RowSignature signature;

  public PolarisExternalTableSpec(
      @NotNull final InputSource inputSource,
      @Nullable final InputFormat inputFormat,
      @Nullable final RowSignature signature)
  {
    this.inputSource = inputSource;
    this.inputFormat = inputFormat;
    this.signature = signature;
  }

  @NotNull
  InputSource getInputSource()
  {
    return inputSource;
  }

  @Nullable
  InputFormat getInputFormat()
  {
    return inputFormat;
  }

  @Nullable
  RowSignature getSignature()
  {
    return signature;
  }
}
