/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.talaria.counters;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;

import java.io.IOException;
import java.util.Map;

/**
 * Serializer for {@link CounterSnapshots}. Necessary because otherwise the map structure causes Jackson
 * to miss including type codes.
 */
public class CounterSnapshotsSerializer extends StdSerializer<CounterSnapshots>
{
  public CounterSnapshotsSerializer()
  {
    super(CounterSnapshots.class);
  }

  @Override
  public void serialize(
      final CounterSnapshots value,
      final JsonGenerator jg,
      final SerializerProvider serializers
  ) throws IOException
  {
    jg.writeStartObject();

    for (final Map.Entry<String, QueryCounterSnapshot> entry : value.getMap().entrySet()) {
      jg.writeObjectField(entry.getKey(), entry.getValue());
    }

    jg.writeEndObject();
  }
}
