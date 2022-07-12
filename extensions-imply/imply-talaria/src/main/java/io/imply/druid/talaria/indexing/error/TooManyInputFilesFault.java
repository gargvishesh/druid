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
import io.imply.druid.talaria.util.TalariaContext;

import java.util.Objects;

@JsonTypeName(TooManyInputFilesFault.CODE)
public class TooManyInputFilesFault extends BaseTalariaFault
{
  static final String CODE = "TooManyInputFiles";

  private final int numInputFiles;
  private final int maxInputFiles;
  private final int minNumWorkers;


  @JsonCreator
  public TooManyInputFilesFault(
      @JsonProperty("numInputFiles") final int numInputFiles,
      @JsonProperty("maxInputFiles") final int maxInputFiles,
      @JsonProperty("minNumWorkers") final int minNumWorkers
  )
  {
    super(
        CODE,
        "Too many input files/segments [%d] encountered. Maximum input files/segments per worker is set to [%d]. Try"
        + " breaking your query up into smaller queries, or increasing the number of workers to at least [%d] by"
        + " setting %s in your query context",
        numInputFiles,
        maxInputFiles,
        minNumWorkers,
        TalariaContext.CTX_MAX_NUM_TASKS
    );
    this.numInputFiles = numInputFiles;
    this.maxInputFiles = maxInputFiles;
    this.minNumWorkers = minNumWorkers;
  }

  @JsonProperty
  public int getNumInputFiles()
  {
    return numInputFiles;
  }

  @JsonProperty
  public int getMaxInputFiles()
  {
    return maxInputFiles;
  }

  @JsonProperty
  public int getMinNumWorkers()
  {
    return minNumWorkers;
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
    TooManyInputFilesFault that = (TooManyInputFilesFault) o;
    return numInputFiles == that.numInputFiles
           && maxInputFiles == that.maxInputFiles
           && minNumWorkers == that.minNumWorkers;
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(super.hashCode(), numInputFiles, maxInputFiles, minNumWorkers);
  }
}
