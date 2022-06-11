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
import org.apache.druid.segment.column.ColumnHolder;

@JsonTypeName(InsertTimeNullFault.CODE)
public class InsertTimeNullFault extends BaseTalariaFault
{
  public static final InsertTimeNullFault INSTANCE = new InsertTimeNullFault();
  static final String CODE = "InsertTimeNull";

  InsertTimeNullFault()
  {
    super(CODE, "Null timestamp (%s) encountered during INSERT or REPLACE.", ColumnHolder.TIME_COLUMN_NAME);
  }

  @JsonCreator
  public static InsertTimeNullFault instance()
  {
    return INSTANCE;
  }
}
