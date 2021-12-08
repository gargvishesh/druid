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

@JsonTypeName(QueryNotSupportedFault.CODE)
public class QueryNotSupportedFault extends BaseTalariaFault
{
  public static final QueryNotSupportedFault INSTANCE = new QueryNotSupportedFault();
  static final String CODE = "QueryNotSupported";

  QueryNotSupportedFault()
  {
    super(CODE);
  }

  @JsonCreator
  public static QueryNotSupportedFault instance()
  {
    return INSTANCE;
  }
}
