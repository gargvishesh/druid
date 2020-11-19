/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.ingest.metadata;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import org.apache.druid.data.input.InputFormat;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.TimestampSpec;

import javax.annotation.Nullable;
import java.util.Objects;

public class IngestSchema
{
  protected final TimestampSpec timestampSpec;
  protected final DimensionsSpec dimensionsSpec;
  protected final InputFormat inputFormat;

  @JsonCreator
  public IngestSchema(
      @JsonProperty("timestampSpec") @Nullable TimestampSpec timestampSpec,
      @JsonProperty("dimensionsSpec") @Nullable DimensionsSpec dimensionsSpec,
      @JsonProperty("inputFormat") @Nullable InputFormat inputFormat
  )
  {
    this.timestampSpec = timestampSpec == null
                         ? new TimestampSpec(null, null, null)
                         : timestampSpec;
    this.dimensionsSpec = dimensionsSpec;
    this.inputFormat = Preconditions.checkNotNull(inputFormat, "'inputFormat' must be specified");
  }

  @JsonProperty("timestampSpec")
  public TimestampSpec getTimestampSpec()
  {
    return timestampSpec;
  }


  @JsonProperty("dimensionsSpec")
  public DimensionsSpec getDimensionsSpec()
  {
    return dimensionsSpec;
  }

  @JsonProperty("inputFormat")
  public InputFormat getInputFormat()
  {
    return inputFormat;
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
    IngestSchema that = (IngestSchema) o;
    return Objects.equals(timestampSpec, that.timestampSpec) &&
           Objects.equals(dimensionsSpec, that.dimensionsSpec) &&
           Objects.equals(inputFormat, that.inputFormat);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(timestampSpec, dimensionsSpec, inputFormat);
  }
}
