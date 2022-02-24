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

@JsonTypeName(NotEnoughMemoryFault.CODE)
public class NotEnoughMemoryFault extends BaseTalariaFault
{
  static final String CODE = "NotEnoughMemory";

  private final long serverMemory;
  private final int serverWorkers;
  private final int serverThreads;

  @JsonCreator
  public NotEnoughMemoryFault(
      @JsonProperty("serverMemory") final long serverMemory,
      @JsonProperty("serverWorkers") final int serverWorkers,
      @JsonProperty("serverThreads") final int serverThreads
  )
  {
    super(
        CODE,
        "Not enough memory (available = %,d; server workers = %,d; server threads = %,d)",
        serverMemory,
        serverWorkers,
        serverThreads
    );

    this.serverMemory = serverMemory;
    this.serverWorkers = serverWorkers;
    this.serverThreads = serverThreads;
  }

  @JsonProperty
  public long getServerMemory()
  {
    return serverMemory;
  }

  @JsonProperty
  public int getServerWorkers()
  {
    return serverWorkers;
  }

  @JsonProperty
  public int getServerThreads()
  {
    return serverThreads;
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
    NotEnoughMemoryFault that = (NotEnoughMemoryFault) o;
    return serverMemory == that.serverMemory
           && serverWorkers == that.serverWorkers
           && serverThreads == that.serverThreads;
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(super.hashCode(), serverMemory, serverWorkers, serverThreads);
  }

  @Override
  public String toString()
  {
    return "NotEnoughMemoryFault{" +
           "serverMemory=" + serverMemory +
           ", serverWorkers=" + serverWorkers +
           ", serverThreads=" + serverThreads +
           '}';
  }
}
