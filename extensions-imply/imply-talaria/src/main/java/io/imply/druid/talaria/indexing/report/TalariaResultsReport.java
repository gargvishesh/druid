/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.talaria.indexing.report;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import org.apache.druid.java.util.common.guava.Yielder;
import org.apache.druid.segment.column.RowSignature;

import javax.annotation.Nullable;
import java.util.List;

public class TalariaResultsReport
{
  private final RowSignature signature;
  @Nullable
  private final List<String> sqlTypeNames;
  private final Yielder<Object[]> resultYielder;

  public TalariaResultsReport(
      final RowSignature signature,
      @Nullable final List<String> sqlTypeNames,
      final Yielder<Object[]> resultYielder
  )
  {
    this.signature = Preconditions.checkNotNull(signature, "signature");
    this.sqlTypeNames = sqlTypeNames;
    this.resultYielder = Preconditions.checkNotNull(resultYielder, "resultYielder");
  }

  /**
   * Method that prevents Jackson deserialization, because serialization is one-way for this class.
   */
  @JsonCreator
  static TalariaResultsReport dontCreate()
  {
    throw new UnsupportedOperationException();
  }

  @JsonProperty("signature")
  public RowSignature getSignature()
  {
    return signature;
  }

  @JsonProperty("sqlTypeNames")
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public List<String> getSqlTypeNames()
  {
    return sqlTypeNames;
  }

  @JsonProperty("results")
  public Yielder<Object[]> getResultYielder()
  {
    return resultYielder;
  }
}
