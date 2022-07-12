/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.talaria.input;

import com.fasterxml.jackson.annotation.JsonTypeName;

/**
 * An input slice that represents nothing.
 */
@JsonTypeName("nil")
public class NilInputSlice implements InputSlice
{
  public static final NilInputSlice INSTANCE = new NilInputSlice();

  private NilInputSlice()
  {
    // Singleton.
  }

  @Override
  public int numFiles()
  {
    return 0;
  }

  @Override
  public boolean equals(Object obj)
  {
    return obj != null && obj.getClass() == this.getClass();
  }

  @Override
  public int hashCode()
  {
    return 0;
  }

  @Override
  public String toString()
  {
    return "NilInputSlice";
  }
}
