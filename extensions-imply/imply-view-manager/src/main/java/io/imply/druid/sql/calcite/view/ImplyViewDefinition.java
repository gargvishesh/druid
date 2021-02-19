/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.sql.calcite.view;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;

public class ImplyViewDefinition
{
  private final String viewName;
  private final String viewSql;

  @JsonCreator
  public ImplyViewDefinition(
      @JsonProperty("viewName") String viewName,
      @JsonProperty("viewSql") String viewSql
  )
  {
    this.viewName = viewName;
    this.viewSql = viewSql;
  }

  @JsonProperty
  public String getViewName()
  {
    return viewName;
  }

  @JsonProperty
  public String getViewSql()
  {
    return viewSql;
  }

  @Override
  public String toString()
  {
    return "ImplyViewDefinition{" +
           "viewName='" + viewName + '\'' +
           ", viewSql='" + viewSql + '\'' +
           '}';
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    ImplyViewDefinition that = (ImplyViewDefinition) o;
    return Objects.equals(getViewName(), that.getViewName()) &&
           Objects.equals(getViewSql(), that.getViewSql());
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(getViewName(), getViewSql());
  }
}
