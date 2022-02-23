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

import java.util.Objects;

@JsonTypeName(TooManyWorkersFault.CODE)
public class TooManyWorkersFault extends BaseTalariaFault
{
  static final String CODE = "TooManyWorkers";

  private final int workers;
  private final int maxWorkers;

  @JsonCreator
  public TooManyWorkersFault(
      @JsonProperty("workers") final int workers,
      @JsonProperty("maxWorkers") final int maxWorkers
  )
  {
    super(CODE, "Too many workers (current = %d; max = %d)", workers, maxWorkers);
    this.workers = workers;
    this.maxWorkers = maxWorkers;
  }

  @JsonProperty
  public int getWorkers()
  {
    return workers;
  }

  @JsonProperty
  public int getMaxWorkers()
  {
    return maxWorkers;
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
    TooManyWorkersFault that = (TooManyWorkersFault) o;
    return workers == that.workers && maxWorkers == that.maxWorkers;
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(super.hashCode(), workers, maxWorkers);
  }

  @Override
  public String toString()
  {
    return "TooManyWorkersFault{" +
           "workers=" + workers +
           ", maxWorkers=" + maxWorkers +
           '}';
  }
}
