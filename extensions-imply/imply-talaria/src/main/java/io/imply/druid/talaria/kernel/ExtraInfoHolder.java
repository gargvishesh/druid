/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.talaria.kernel;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

import javax.annotation.Nullable;
import java.util.Objects;

/**
 * Holds extra info that will be passed as the "extra" parameter to
 * {@link io.imply.druid.talaria.frame.processor.FrameProcessorFactory#makeProcessors}.
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
public abstract class ExtraInfoHolder<ExtraInfoType>
{
  public static final String INFO_KEY = "info";

  @Nullable
  private final ExtraInfoType extra;

  public ExtraInfoHolder(@Nullable final ExtraInfoType extra)
  {
    this.extra = extra;
  }

  @JsonProperty(INFO_KEY)
  @JsonInclude(JsonInclude.Include.NON_NULL)
  @Nullable
  public ExtraInfoType getExtraInfo()
  {
    return extra;
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
    ExtraInfoHolder<?> that = (ExtraInfoHolder<?>) o;
    return Objects.equals(extra, that.extra);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(extra);
  }

  @Override
  public String toString()
  {
    return "ExtraInfoHolder{" +
           "extra=" + extra +
           '}';
  }
}
