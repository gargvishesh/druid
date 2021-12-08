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
import com.google.common.base.Preconditions;

@JsonTypeName(InsertCannotBeEmptyFault.CODE)
public class InsertCannotBeEmptyFault extends BaseTalariaFault
{
  static final String CODE = "InsertCannotBeEmpty";

  private final String dataSource;

  @JsonCreator
  public InsertCannotBeEmptyFault(
      @JsonProperty("dataSource") final String dataSource
  )
  {
    super(CODE, "No rows to insert for dataSource [%s]", dataSource);
    this.dataSource = Preconditions.checkNotNull(dataSource, "dataSource");
  }

  @JsonProperty
  public String getDataSource()
  {
    return dataSource;
  }
}
