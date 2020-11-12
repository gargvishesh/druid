/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc.
 */

package io.imply.druid.cloudwatch;


import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.druid.data.input.ByteBufferInputRowParser;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.MapBasedInputRow;
import org.apache.druid.data.input.impl.ParseSpec;
import org.apache.druid.data.input.impl.StringInputRowParser;
import org.apache.druid.guice.annotations.Json;
import org.apache.druid.java.util.common.parsers.ParseException;
import org.apache.druid.utils.CompressionUtils;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;

/**
 * Parses CloudWatch subscription filter data.
 *
 * See https://docs.aws.amazon.com/AmazonCloudWatch/latest/logs/SubscriptionFilters.html for details.
 */
public class CloudWatchInputRowParser implements ByteBufferInputRowParser
{
  public static final String TYPE_NAME = "cloudwatch";

  private final ParseSpec parseSpec;
  private final ObjectMapper jsonMapper;
  private final StringInputRowParser stringParser;

  @JsonCreator
  public CloudWatchInputRowParser(
      @JsonProperty("parseSpec") final ParseSpec parseSpec,
      @JacksonInject @Json ObjectMapper jsonMapper
  )
  {
    this.parseSpec = parseSpec;
    this.jsonMapper = jsonMapper.copy();
    this.stringParser = new StringInputRowParser(parseSpec, StandardCharsets.UTF_8.name());
  }

  @Override
  public ByteBufferInputRowParser withParseSpec(final ParseSpec parseSpec)
  {
    return new CloudWatchInputRowParser(parseSpec, jsonMapper);
  }

  @Override
  public ParseSpec getParseSpec()
  {
    return parseSpec;
  }

  @Override
  public List<InputRow> parseBatch(final ByteBuffer buffer)
  {
    try {
      // Gunzip.
      final byte[] bytes = new byte[buffer.remaining()];
      buffer.get(bytes);
      final ByteArrayOutputStream baos = new ByteArrayOutputStream();
      CompressionUtils.gunzip(new ByteArrayInputStream(bytes), baos);
      final byte[] decompressedBytes = baos.toByteArray();

      // JSON decode.
      final CloudWatchLogHolder holder = jsonMapper.readValue(decompressedBytes, CloudWatchLogHolder.class);

      // Parse and return a batch of log events.
      final List<InputRow> rows = new ArrayList<>();
      for (CloudWatchLogEvent logEvent : holder.getLogEvents()) {
        final InputRow row = stringParser.parse(logEvent.getMessage());

        if (row instanceof MapBasedInputRow) {
          final Map<String, Object> m = ((MapBasedInputRow) row).getEvent();

          if (m instanceof HashMap || m instanceof TreeMap) {
            // Definitely mutable.
            addHolderFieldsToMap(m, holder);
            rows.add(row);
          } else {
            // Maybe not mutable; make a copy.
            final Map<String, Object> newMap = new HashMap<>(m);
            addHolderFieldsToMap(newMap, holder);
            rows.add(new MapBasedInputRow(row.getTimestamp(), row.getDimensions(), newMap));
          }
        } else {
          // Can't extract and adjust the map. Just pass it along as-is without extra goodies.
          rows.add(row);
        }
      }

      return rows;
    }
    catch (IOException e) {
      throw new ParseException(e, "Could not decode message");
    }
  }

  private void addHolderFieldsToMap(final Map<String, Object> m, final CloudWatchLogHolder holder)
  {
    m.put("owner", holder.getOwner());
    m.put("logGroup", holder.getLogGroup());
    m.put("logStream", holder.getLogStream());
  }

  @Override
  public boolean equals(final Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final CloudWatchInputRowParser that = (CloudWatchInputRowParser) o;
    return Objects.equals(parseSpec, that.parseSpec);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(parseSpec);
  }

  @Override
  public String toString()
  {
    return "CloudWatchInputRowParser{" +
           "parseSpec=" + parseSpec +
           '}';
  }
}
