/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.talaria.frame.cluster;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.google.common.collect.ImmutableList;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * A Jackson deserializer that is created specifically for a particular signature + cluster key combination. This
 * is necessary because otherwise, types can be deserialized incorrectly. Longs might get deserialized as ints, etc.
 */
public class ClusterByKeyDeserializer extends JsonDeserializer<ClusterByKey>
{
  private final List<ColumnType> types;

  private ClusterByKeyDeserializer(final List<ColumnType> types)
  {
    this.types = types;
  }

  public static ClusterByKeyDeserializer fromClusterByAndRowSignature(
      final ClusterBy clusterBy,
      final RowSignature rowSignature
  )
  {
    final List<ColumnType> types = new ArrayList<>(clusterBy.getColumns().size());

    for (ClusterByColumn part : clusterBy.getColumns()) {
      final String name = part.columnName();
      final ColumnType type = rowSignature.getColumnType(name)
                                          .orElseThrow(() -> new ISE("No type for column [%s]", name));

      types.add(type);
    }

    return new ClusterByKeyDeserializer(types);
  }

  @Override
  public ClusterByKey deserialize(
      final JsonParser jp,
      final DeserializationContext ctxt
  ) throws IOException
  {
    int partNum = 0;
    if (jp.isExpectedStartArrayToken()) {
      final Object[] retVal = new Object[types.size()];

      boolean encounteredEndArray = false;
      for (; partNum < types.size(); partNum++) {
        final JsonToken token = jp.nextToken();

        if (token == JsonToken.END_ARRAY) {
          // Short key read, can happen if this is a bucket key instead of a full cluster key.
          encounteredEndArray = true;
          break;
        }

        switch (types.get(partNum).getType()) {
          case ARRAY:
            switch (types.get(partNum).getElementType().getType()) {
              case STRING:
                extractString(jp, ctxt, retVal, partNum, token, true);
                break;
              default:
                throw new ISE("Can't handle type [%s]", types.get(partNum).asTypeString());
            }
            break;

          case STRING:
            extractString(jp, ctxt, retVal, partNum, token, false);
            break;

          case LONG:
            retVal[partNum] = token == JsonToken.VALUE_NULL ? null : jp.getLongValue();
            break;

          case FLOAT:
            retVal[partNum] = token == JsonToken.VALUE_NULL ? null : jp.getFloatValue();
            break;

          case DOUBLE:
            retVal[partNum] = token == JsonToken.VALUE_NULL ? null : jp.getDoubleValue();
            break;

          default:
            throw new ISE("Can't handle type [%s]", types.get(partNum).asTypeString());
        }
      }

      if (!encounteredEndArray && jp.nextToken() != JsonToken.END_ARRAY) {
        throw ctxt.wrongTokenException(jp, ClusterByKey.class, JsonToken.END_ARRAY, null);
      }

      // Trim in case of short key read.
      return ClusterByKey.of(retVal).trim(partNum);
    } else {
      return (ClusterByKey) ctxt.handleUnexpectedToken(ClusterByKey.class, jp);
    }
  }

  private void extractString(
      JsonParser jp,
      DeserializationContext ctxt,
      Object[] retVal,
      int i,
      JsonToken token,
      boolean toArray
  )
      throws IOException
  {
    if (token == JsonToken.VALUE_NULL) {
      retVal[i] = null;
    } else if (token == JsonToken.VALUE_STRING) {
      retVal[i] = toArray ? ImmutableList.of(jp.getText()) : jp.getText();
    } else if (token == JsonToken.START_ARRAY) {
      final List<String> strings = new ArrayList<>();

      while (jp.nextToken() != JsonToken.END_ARRAY) {
        strings.add(jp.getText());
      }

      retVal[i] = strings;
    } else {
      throw ctxt.instantiationException(
          ClusterByKey.class,
          StringUtils.format("Unexpected token [%s] when reading string", token)
      );
    }
  }
}
