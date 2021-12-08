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
import com.fasterxml.jackson.annotation.JsonTypeName;

@JsonTypeName(UnknownFault.CODE)
public class UnknownFault extends BaseTalariaFault
{
  public static final UnknownFault INSTANCE = new UnknownFault();
  static final String CODE = "UnknownError";

  UnknownFault()
  {
    super(CODE);
  }

  @JsonCreator
  public static UnknownFault instance()
  {
    return INSTANCE;
  }
}
