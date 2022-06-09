/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.nested.column;

import org.apache.druid.java.util.common.IAE;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;

public class PathFinder
{
  public static String toNormalizedJsonPath(List<PathPart> paths)
  {
    if (paths.isEmpty()) {
      return "$.";
    }
    StringBuilder bob = new StringBuilder();
    boolean first = true;
    for (PathPart partFinder : paths) {
      if (partFinder instanceof MapField) {
        if (first) {
          bob.append("$.");
        } else {
          bob.append(".");
        }
        final String id = partFinder.getPartName();
        if (id.contains(".") || id.contains("'") || id.contains("\"") || id.contains("[") || id.contains("]")) {
          bob.append("['").append(id).append("']");
        } else {
          bob.append(id);
        }
      } else if (partFinder instanceof ArrayElement) {
        if (first) {
          bob.append("$");
        }
        bob.append("[").append(partFinder.getPartName()).append("]");
      }
      first = false;
    }
    return bob.toString();
  }

  /**
   * split a JSONPath path into a series of extractors to find things in stuff
   */
  public static List<PathPart> parseJsonPath(@Nullable String path)
  {
    if (path == null || path.isEmpty()) {
      return Collections.emptyList();
    }
    List<PathPart> parts = new ArrayList<>();

    if (!path.startsWith("$")) {
      badFormatJsonPath(path, "must start with '$'");
    }

    if (path.length() == 1) {
      return Collections.emptyList();
    }

    int partMark = -1;  // position to start the next substring to build the path part
    int dotMark = -1;   // last position where a '.' character was encountered, indicating the start of a new part
    int arrayMark = -1; // position of leading '[' indicating start of array (or field name if ' immediately follows)
    int quoteMark = -1; // position of leading ', indicating a quoted field name

    // start at position 1 since $ is special
    for (int i = 1; i < path.length(); i++) {
      final char current = path.charAt(i);
      if (current == '.' && arrayMark < 0 && quoteMark < 0) {
        if (dotMark >= 0) {
          parts.add(new MapField(getPathSubstring(path, partMark, i)));
        }
        dotMark = i;
        partMark = i + 1;
      } else if (current == '[' && arrayMark < 0 && quoteMark < 0) {
        if (dotMark == (i - 1) && dotMark != 0) {
          badFormatJsonPath(path, "invalid position " + i + " for '[', must not follow '.' or must be contained with '");
        }
        if (dotMark >= 0 && i > 1) {
          parts.add(new MapField(getPathSubstring(path, partMark, i)));
          dotMark = -1;
        }
        arrayMark = i;
        partMark = i + 1;
      } else if (current == ']' && arrayMark >= 0 && quoteMark < 0) {
        String maybeNumber = getPathSubstring(path, partMark, i);
        Integer index;
        try {
          index = Integer.parseInt(maybeNumber);
          parts.add(new ArrayElement(index));
          dotMark = -1;
          arrayMark = -1;
          partMark = i + 1;
        }
        catch (NumberFormatException ignored) {
          badFormatJsonPath(path, "expected number for array specifier got " + maybeNumber + " instead. Use ' if this value was meant to be a field name");
        }
      } else if (dotMark == -1 && arrayMark == -1) {
        badFormatJsonPath(path, "path parts must be separated with '.'");
      } else if (current == '\'' && quoteMark < 0) {
        if (arrayMark != i - 1) {
          badFormatJsonPath(path, "' must be immediately after '['");
        }
        quoteMark = i;
        partMark = i + 1;
      } else if (current == '\'' && quoteMark >= 0 && path.charAt(i - 1) != '\\') {
        parts.add(new MapField(getPathSubstring(path, partMark, i)));
        dotMark = -1;
        quoteMark = -1;
        // chomp to next char to eat closing array
        if (++i == path.length()) {
          break;
        }
        if (path.charAt(i) != ']') {
          badFormatJsonPath(path, "closing ' must immediately precede ']'");
        }
        partMark = i + 1;
        arrayMark = -1;
      }
    }
    // add the last element, this should never be an array because they close themselves
    if (partMark < path.length()) {
      if (quoteMark != -1) {
        badFormatJsonPath(path, "unterminated '");
      }
      if (arrayMark != -1) {
        badFormatJsonPath(path, "unterminated '['");
      }
      parts.add(new MapField(path.substring(partMark)));
    }

    return parts;
  }

  /**
   * Given a list of part finders, convert it to a "normalized" 'jq' path format that is consistent with how
   * {@link StructuredDataProcessor} constructs field path names
   */
  public static String toNormalizedJqPath(List<PathPart> paths)
  {
    if (paths.isEmpty()) {
      return StructuredDataProcessor.ROOT_LITERAL;
    }
    StringBuilder bob = new StringBuilder();
    boolean first = true;
    for (PathPart partFinder : paths) {
      if (partFinder instanceof MapField) {
        bob.append(".");
        bob.append("\"").append(partFinder.getPartName()).append("\"");
      } else {
        if (first) {
          bob.append(".");
        }
        bob.append("[").append(partFinder.getPartName()).append("]");
      }
      first = false;
    }
    return bob.toString();
  }

  /**
   * split a jq path into a series of extractors to find things in stuff
   */
  public static List<PathPart> parseJqPath(@Nullable String path)
  {
    if (path == null || path.isEmpty()) {
      return Collections.emptyList();
    }
    List<PathPart> parts = new ArrayList<>();

    if (path.charAt(0) != '.') {
      badFormat(path, "must start with '.'");
    }

    int partMark = -1;  // position to start the next substring to build the path part
    int dotMark = -1;   // last position where a '.' character was encountered, indicating the start of a new part
    int arrayMark = -1; // position of leading '[' indicating start of array (or field name if '"' immediately follows)
    int quoteMark = -1; // position of leading '"', indicating a quoted field name
    for (int i = 0; i < path.length(); i++) {
      final char current = path.charAt(i);
      if (current == '.' && arrayMark < 0 && quoteMark < 0) {
        if (dotMark >= 0) {
          parts.add(new MapField(getPathSubstring(path, partMark, i)));
        }
        dotMark = i;
        partMark = i + 1;
      } else if (current == '?' && quoteMark < 0) {
        // eat optional marker
        if (partMark != i) {
          if (dotMark >= 0) {
            parts.add(new MapField(getPathSubstring(path, partMark, i)));
            dotMark = -1;
          } else {
            badFormat(path, "invalid position " + i + " for '?'");
          }
        }
        partMark = i + 1;
      } else if (current == '[' && arrayMark < 0 && quoteMark < 0) {
        if (dotMark == (i - 1) && dotMark != 0) {
          badFormat(path, "invalid position " + i + " for '[', must not follow '.' or must be contained with '\"'");
        }
        if (dotMark >= 0 && i > 1) {
          parts.add(new MapField(getPathSubstring(path, partMark, i)));
          dotMark = -1;
        }
        arrayMark = i;
        partMark = i + 1;
      } else if (current == ']' && arrayMark >= 0 && quoteMark < 0) {
        String maybeNumber = getPathSubstring(path, partMark, i);
        Integer index;
        try {
          index = Integer.parseInt(maybeNumber);
          parts.add(new ArrayElement(index));
          dotMark = -1;
          arrayMark = -1;
          partMark = i + 1;
        }
        catch (NumberFormatException ignored) {
          badFormat(path, "expected number for array specifier got " + maybeNumber + " instead. Use \"\" if this value was meant to be a field name");
        }
      } else if (dotMark == -1 && arrayMark == -1) {
        badFormat(path, "path parts must be separated with '.'");
      } else if (current == '"' && quoteMark < 0) {
        if (partMark != i) {
          badFormat(path, "invalid position " + i + " for '\"', must immediately follow '.' or '['");
        }
        if (arrayMark > 0 && arrayMark != i - 1) {
          badFormat(path, "'\"' within '[' must be immediately after");
        }
        quoteMark = i;
        partMark = i + 1;
      } else if (current == '"' && quoteMark >= 0 && path.charAt(i - 1) != '\\') {
        parts.add(new MapField(getPathSubstring(path, partMark, i)));
        dotMark = -1;
        quoteMark = -1;
        if (arrayMark > 0) {
          // chomp to next char to eat closing array
          if (++i == path.length()) {
            break;
          }
          if (path.charAt(i) != ']') {
            badFormat(path, "closing '\"' must immediately precede ']'");
          }
          partMark = i + 1;
          arrayMark = -1;
        } else {
          partMark = i + 1;
        }
      }
    }
    // add the last element, this should never be an array because they close themselves
    if (partMark < path.length()) {
      if (quoteMark != -1) {
        badFormat(path, "unterminated '\"'");
      }
      if (arrayMark != -1) {
        badFormat(path, "unterminated '['");
      }
      parts.add(new MapField(path.substring(partMark)));
    }

    return parts;
  }

  private static String getPathSubstring(String path, int start, int end)
  {
    if (end - start < 1) {
      badFormat(path, "path parts separated by '.' must not be empty");
    }
    return path.substring(start, end);
  }

  private static void badFormat(String path, String message)
  {
    throw new IAE("Bad format, '%s' is not a valid 'jq' path: %s", path, message);
  }

  private static void badFormatJsonPath(String path, String message)
  {
    throw new IAE("Bad format, '%s' is not a valid JSONPath path: %s", path, message);
  }

  /**
   * Dig through a thing to find stuff, if that stuff is a not nested itself
   */
  @Nullable
  public static String findStringLiteral(@Nullable Object data, List<PathPart> path)
  {
    Object currentObject = find(data, path);
    if (currentObject instanceof Map || currentObject instanceof List || currentObject instanceof Object[]) {
      return null;
    } else {
      // a literal of some sort, huzzah!
      if (currentObject == null) {
        return null;
      }
      return String.valueOf(currentObject);
    }
  }

  @Nullable
  public static Object findLiteral(@Nullable Object data, List<PathPart> path)
  {
    Object currentObject = find(data, path);
    if (currentObject instanceof Map || currentObject instanceof List || currentObject instanceof Object[]) {
      return null;
    } else {
      // a literal of some sort, huzzah!
      if (currentObject == null) {
        return null;
      }
      return currentObject;
    }
  }

  @Nullable
  public static Object[] findKeys(@Nullable Object data, List<PathPart> path)
  {
    Object currentObject = find(data, path);
    if (currentObject instanceof Map) {
      return ((Map) currentObject).keySet().toArray();
    }
    if (currentObject instanceof List) {
      return IntStream.range(0, ((List) currentObject).size()).mapToObj(Integer::toString).toArray();
    }
    if (currentObject instanceof Object[]) {
      return IntStream.range(0, ((Object[]) currentObject).length).mapToObj(Integer::toString).toArray();
    }
    return null;
  }

  /**
   * Dig through a thing to find stuff
   */
  @Nullable
  public static Object find(@Nullable Object data, List<PathPart> path)
  {
    Object currentObject = data;
    for (PathPart pathPart : path) {
      Object objectAtPath = pathPart.find(currentObject);
      if (objectAtPath == null) {
        return null;
      }
      currentObject = objectAtPath;
    }
    return currentObject;
  }

  public interface PathPart
  {
    @Nullable
    Object find(@Nullable Object input);

    String getPartName();
  }

  static class MapField implements PathPart
  {
    private final String path;

    MapField(String path)
    {
      this.path = path;
    }

    @Override
    public Object find(Object input)
    {
      if (input instanceof Map) {
        Map<String, Object> currentMap = (Map<String, Object>) input;
        return currentMap.get(path);
      }
      return null;
    }

    @Override
    public String getPartName()
    {
      return path;
    }
  }

  static class ArrayElement implements PathPart
  {
    private final int index;

    ArrayElement(int index)
    {
      this.index = index;
    }

    @Nullable
    @Override
    public Object find(@Nullable Object input)
    {
      // handle lists or arrays because who knows what might end up here, depending on how is created
      if (input instanceof List) {
        List<?> currentList = (List<?>) input;
        if (currentList.size() > index) {
          return currentList.get(index);
        }
      } else if (input instanceof Object[]) {
        Object[] currentList = (Object[]) input;
        if (currentList.length > index) {
          return currentList[index];
        }
      }
      return null;
    }

    @Override
    public String getPartName()
    {
      return String.valueOf(index);
    }
  }
}
