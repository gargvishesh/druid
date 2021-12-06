/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.talaria.frame.cluster.statistics;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;

import java.io.IOException;

public class KeyCollectorSnapshotDeserializer extends JsonDeserializer<KeyCollectorSnapshot>
{
  private final KeyCollectorFactory<?> keyCollectorFactory;

  public KeyCollectorSnapshotDeserializer(final KeyCollectorFactory<?> keyCollectorFactory)
  {
    this.keyCollectorFactory = keyCollectorFactory;
  }

  @Override
  public KeyCollectorSnapshot deserialize(JsonParser jp, DeserializationContext ctxt) throws IOException
  {
    return jp.readValueAs(keyCollectorFactory.snapshotClass());
  }
}
