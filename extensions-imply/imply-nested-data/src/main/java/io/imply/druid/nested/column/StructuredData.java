/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.nested.column;

import com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.druid.java.util.common.guava.Comparators;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class StructuredData implements Comparable<StructuredData>
{
  public static final Comparator<StructuredData> COMPARATOR = Comparators.naturalNullsFirst();

  private final Object value;

  @JsonCreator
  public static StructuredData create(Object value)
  {
    return new StructuredData(value);
  }

  public StructuredData(Object value)
  {
    this.value = value;
  }

  public Object getValue()
  {
    return value;
  }

  @Override
  public int compareTo(StructuredData o)
  {
    if (Objects.equals(value, o.value)) {
      return 0;
    }

    // Note: We just want it to be deterministic
    return value.hashCode() <= o.value.hashCode() ? -1 : 1;
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
    StructuredData that = (StructuredData) o;
    return Objects.equals(value, that.value);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(value);
  }

  @Override
  public String toString()
  {
    return "StructuredData{" +
           "value=" + value +
           '}';
  }

  public long estimateSize()
  {
    if (value instanceof Map) {
      return ((Map) value).size() * (2 * Long.BYTES + 256);
    }
    if (value instanceof List) {
      return ((List) value).size() * (2 * Long.BYTES + 256);
    }
    return 0;
  }
}
