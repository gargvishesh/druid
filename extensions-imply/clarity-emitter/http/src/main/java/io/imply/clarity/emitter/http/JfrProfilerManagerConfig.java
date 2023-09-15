/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.clarity.emitter.http;

import java.util.HashMap;
import java.util.Map;

public class JfrProfilerManagerConfig
{
  private final Map<String, String> tags;

  public JfrProfilerManagerConfig(Map<String, String> tags)
  {
    this.tags = tags;
  }

  public static Builder builder()
  {
    return new Builder();
  }

  public Map<String, String> getTags()
  {
    return tags;
  }

  public static class Builder
  {
    private final Map<String, String> tags = new HashMap<>();

    public Builder addTag(String key, String value)
    {
      tags.put(key, value);
      return this;
    }

    public Builder addTags(Map<String, String> tags)
    {
      this.tags.putAll(tags);
      return this;
    }

    public JfrProfilerManagerConfig build()
    {
      return new JfrProfilerManagerConfig(tags);
    }
  }
}
