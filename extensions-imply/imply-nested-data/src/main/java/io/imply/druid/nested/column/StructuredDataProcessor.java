/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.nested.column;

import io.netty.util.SuppressForbidden;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;

@SuppressForbidden(reason = "Lists#newLinkedList")
public abstract class StructuredDataProcessor
{
  public static final String ROOT_LITERAL = ".";
  private static final List<String> ROOT_LITERAL_FIELDS = Collections.singletonList(ROOT_LITERAL);

  public abstract void processLiteralField(String fieldName, Object fieldValue);

  /**
   * Process fields, returning a list of all "normalized" 'jq' paths to literal fields, consistent with the output of
   * {@link PathFinder#toNormalizedJqPath(List)}.
   */
  public List<String> processFields(Object raw)
  {
    Queue<Field> toProcess = new LinkedList<>();
    if (raw instanceof Map) {
      toProcess.add(new MapField("", (Map<String, ?>) raw));
    } else if (raw instanceof List) {
      toProcess.add(new ListField("", (List<?>) raw));
    } else {
      processLiteralField(ROOT_LITERAL, raw);
      return ROOT_LITERAL_FIELDS;
    }

    List<String> fields = new LinkedList<>();

    while (!toProcess.isEmpty()) {
      Field next = toProcess.poll();
      if (next instanceof MapField) {
        fields.addAll(processMapField(toProcess, (MapField) next));
      } else if (next instanceof ListField) {
        fields.addAll(processListField(toProcess, (ListField) next));
      }
    }
    return fields;
  }

  private List<String> processMapField(Queue<Field> toProcess, MapField map)
  {
    List<String> fields = new LinkedList<>();
    for (Map.Entry<String, ?> entry : map.getMap().entrySet()) {
      final String fieldName = map.getName() + ".\"" + entry.getKey() + "\"";
      if (entry.getValue() instanceof List) {
        List<?> theList = (List<?>) entry.getValue();
        toProcess.add(new ListField(fieldName, theList));
      } else if (entry.getValue() instanceof Map) {
        toProcess.add(new MapField(fieldName, (Map<String, ?>) entry.getValue()));
      } else {
        fields.add(fieldName);
        // must be a literal
        processLiteralField(fieldName, entry.getValue());
      }
    }
    return fields;
  }

  private List<String> processListField(Queue<Field> toProcess, ListField list)
  {
    List<String> fields = new LinkedList<>();
    final List<?> theList = list.getList();
    for (int i = 0; i < theList.size(); i++) {
      final String listFieldName = list.getName() + ".[" + i + "]";
      final Object element = theList.get(i);
      if (element instanceof Map) {
        toProcess.add(new MapField(listFieldName, (Map<String, ?>) element));
      } else if (element instanceof List) {
        toProcess.add(new ListField(listFieldName, (List<?>) element));
      } else {
        fields.add(listFieldName);
        processLiteralField(listFieldName, element);
      }
    }
    return fields;
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
}
