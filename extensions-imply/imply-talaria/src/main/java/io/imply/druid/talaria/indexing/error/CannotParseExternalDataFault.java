/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.talaria.indexing.error;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.base.Preconditions;

@JsonTypeName(CannotParseExternalDataFault.CODE)
public class CannotParseExternalDataFault extends BaseTalariaFault
{
  static final String CODE = "CannotParseExternalData";

  public CannotParseExternalDataFault(@JsonProperty("errorMessage") String message)
  {
    super(CODE, Preconditions.checkNotNull(message, "errorMessage"));
  }
}
