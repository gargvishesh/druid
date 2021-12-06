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
}
