/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.nested.column;

import javax.annotation.Nullable;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Queue;

public abstract class StructuredDataProcessor
{
  public static final String ROOT_LITERAL = ".";
  private static final List<String> ROOT_LITERAL_FIELDS = Collections.singletonList(ROOT_LITERAL);

  public abstract int processLiteralField(String fieldName, Object fieldValue);

  /**
   * Process fields, returning a list of all "normalized" 'jq' paths to literal fields, consistent with the output of
   * {@link PathFinder#toNormalizedJqPath(List)}.
   */
  public ProcessResults processFields(Object raw)
  {
    Queue<Field> toProcess = new ArrayDeque<>();
    if (raw instanceof Map) {
      toProcess.add(new MapField("", (Map<String, ?>) raw));
    } else if (raw instanceof List) {
      toProcess.add(new ListField(ROOT_LITERAL, (List<?>) raw));
    } else {
      return new ProcessResults().withFields(ROOT_LITERAL_FIELDS).withSize(processLiteralField(ROOT_LITERAL, raw));
    }

    ProcessResults accumulator = new ProcessResults();

    while (!toProcess.isEmpty()) {
      Field next = toProcess.poll();
      if (next instanceof MapField) {
        accumulator.merge(processMapField(toProcess, (MapField) next));
      } else if (next instanceof ListField) {
        accumulator.merge(processListField(toProcess, (ListField) next));
      }
    }
    return accumulator;
  }

  private ProcessResults processMapField(Queue<Field> toProcess, MapField map)
  {
    // just guessing a size for a Map as some constant, it might be bigger than this...
    ProcessResults processResults = new ProcessResults().withSize(16);
    for (Map.Entry<String, ?> entry : map.getMap().entrySet()) {
      // add estimated size of string key
      processResults.addSize(estimateStringSize(entry.getKey()));
      final String fieldName = map.getName() + ".\"" + entry.getKey() + "\"";
      // lists and maps go back in the queue
      if (entry.getValue() instanceof List) {
        List<?> theList = (List<?>) entry.getValue();
        toProcess.add(new ListField(fieldName, theList));
      } else if (entry.getValue() instanceof Map) {
        toProcess.add(new MapField(fieldName, (Map<String, ?>) entry.getValue()));
      } else {
        // literals get processed
        processResults.addLiteralField(fieldName, processLiteralField(fieldName, entry.getValue()));
      }
    }
    return processResults;
  }

  private ProcessResults processListField(Queue<Field> toProcess, ListField list)
  {
    // start with object reference, is probably a bit bigger than this...
    ProcessResults results = new ProcessResults().withSize(8);
    final List<?> theList = list.getList();
    for (int i = 0; i < theList.size(); i++) {
      final String listFieldName = list.getName() + "[" + i + "]";
      final Object element = theList.get(i);
      // maps and lists go back into the queue
      if (element instanceof Map) {
        toProcess.add(new MapField(listFieldName, (Map<String, ?>) element));
      } else if (element instanceof List) {
        toProcess.add(new ListField(listFieldName, (List<?>) element));
      } else {
        // literals get processed
        results.addLiteralField(listFieldName, processLiteralField(listFieldName, element));
      }
    }
    return results;
  }

  interface Field
  {
    String getName();
  }

  static class ListField implements Field
  {
    private final String name;
    private final List<?> list;

    ListField(String name, List<?> list)
    {
      this.name = name;
      this.list = list;
    }

    @Override
    public String getName()
    {
      return name;
    }

    public List<?> getList()
    {
      return list;
    }
  }

  static class MapField implements Field
  {
    private final String name;
    private final Map<String, ?> map;

    MapField(String name, Map<String, ?> map)
    {
      this.name = name;
      this.map = map;
    }

    @Override
    public String getName()
    {
      return name;
    }

    public Map<String, ?> getMap()
    {
      return map;
    }
  }

  /**
   * Accumulates the list of literal field paths and a rough size estimation for {@link StructuredDataProcessor}
   */
  public static class ProcessResults
  {
    private List<String> literalFields;
    private int estimatedSize;

    public ProcessResults()
    {
      literalFields = new ArrayList<>();
      estimatedSize = 0;
    }

    public List<String> getLiteralFields()
    {
      return literalFields;
    }

    public int getEstimatedSize()
    {
      return estimatedSize;
    }

    public ProcessResults addSize(int size)
    {
      this.estimatedSize += size;
      return this;
    }

    public ProcessResults addLiteralField(String fieldName, int sizeOfValue)
    {
      literalFields.add(fieldName);
      this.estimatedSize += sizeOfValue;
      return this;
    }

    public ProcessResults withFields(List<String> fields)
    {
      this.literalFields = fields;
      return this;
    }

    public ProcessResults withSize(int size)
    {
      this.estimatedSize = size;
      return this;
    }

    public ProcessResults merge(ProcessResults other)
    {
      this.literalFields.addAll(other.literalFields);
      this.estimatedSize += other.estimatedSize;
      return this;
    }
  }

  /**
   * this is copied from {@link org.apache.druid.segment.StringDimensionDictionary#estimateSizeOfValue(String)}
   */
  public static int estimateStringSize(@Nullable String value)
  {
    return value == null ? 0 : 28 + 16 + (2 * value.length());
  }

  public static int getLongObjectEstimateSize()
  {
    // object reference + size of long
    return 8 + Long.BYTES;
  }

  public static int getDoubleObjectEstimateSize()
  {
    // object reference + size of double
    return 8 + Double.BYTES;
  }
}
