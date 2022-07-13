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

@JsonTypeName(CanceledFault.CODE)
public class CanceledFault extends BaseTalariaFault
{
  public static final CanceledFault INSTANCE = new CanceledFault();
  static final String CODE = "Canceled";

  CanceledFault()
  {
    super(CODE, "Query canceled by user or by task shutdown.");
  }

  @JsonCreator
  public static CanceledFault instance()
  {
    return INSTANCE;
  }
}
