/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.talaria.frame.cluster.statistics;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;

import java.util.Objects;

public class QuantilesSketchKeyCollectorSnapshot implements KeyCollectorSnapshot
{
  private final String encodedSketch;

  @JsonCreator
  public QuantilesSketchKeyCollectorSnapshot(String encodedSketch)
  {
    this.encodedSketch = encodedSketch;
  }

  @JsonValue
  public String getEncodedSketch()
  {
    return encodedSketch;
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
    QuantilesSketchKeyCollectorSnapshot that = (QuantilesSketchKeyCollectorSnapshot) o;
    return Objects.equals(encodedSketch, that.encodedSketch);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(encodedSketch);
  }
}
