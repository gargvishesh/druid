/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.talaria.indexing.error;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;

// TODO(gianm): use this somewhere
@JsonTypeName(TooManyInputFilesFault.CODE)
public class TooManyInputFilesFault extends BaseTalariaFault
{
  static final String CODE = "TooManyInputFiles";

  private final int maxInputFiles;

  @JsonCreator
  public TooManyInputFilesFault(@JsonProperty("maxInputFiles") final int maxInputFiles)
  {
    super(CODE, "Too many input files (max = %,d); try breaking your query up into smaller queries", maxInputFiles);
    this.maxInputFiles = maxInputFiles;
  }

  @JsonProperty
  public int getMaxInputFiles()
  {
    return maxInputFiles;
  }
}
