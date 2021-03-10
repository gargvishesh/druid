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
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.StringUtils;
import org.joda.time.DateTime;
import org.joda.time.chrono.ISOChronology;

import javax.annotation.Nullable;
import java.util.Objects;

public class ImplyViewDefinition
{
  public static final String DEFAULT_NAMESPACE = "__global";

  public static String getKey(String viewName, @Nullable String viewNamespace)
  {
    return viewNamespace != null ? StringUtils.format("%s.%s", viewNamespace, viewName) : viewName;
  }

  private final String viewName;
  @Nullable
  private final String viewNamespace;
  private final String viewSql;
  private final DateTime lastModified;
  private final String viewKey;

  @JsonCreator
  public ImplyViewDefinition(
      @JsonProperty("viewName") String viewName,
      @JsonProperty("viewNamespace") @Nullable String viewNamespace,
      @JsonProperty("viewSql") String viewSql,
      @JsonProperty("lastModified") @Nullable DateTime lastModified
  )
  {
    Preconditions.checkArgument(!DEFAULT_NAMESPACE.equals(viewNamespace), "Cannot use reserved namespace");
    this.viewName = viewName;
    this.viewNamespace = viewNamespace;
    this.viewKey = getKey(viewName, viewNamespace);
    this.viewSql = viewSql;
    this.lastModified = lastModified == null ? DateTimes.nowUtc() : lastModified.toDateTime(ISOChronology.getInstanceUTC());
  }

  public ImplyViewDefinition(String viewName, String viewSql)
  {
    this(viewName, null, viewSql, null);
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

  @JsonProperty
  @Nullable
  public String getViewNamespace()
  {
    return viewNamespace;
  }

  public String getViewNamespaceOrDefault()
  {
    return viewNamespace == null ? DEFAULT_NAMESPACE : viewNamespace;
  }

  @JsonProperty
  public DateTime getLastModified()
  {
    return lastModified;
  }

  public String getViewKey()
  {
    return viewKey;
  }

  @VisibleForTesting
  public ImplyViewDefinition withSql(String viewSql)
  {
    return new ImplyViewDefinition(viewName, viewNamespace, viewSql, DateTimes.nowUtc());
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
    return viewName.equals(that.viewName)
           && Objects.equals(viewNamespace, that.viewNamespace)
           && viewSql.equals(that.viewSql);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(viewName, viewNamespace, viewSql);
  }

  @Override
  public String toString()
  {
    return "ImplyViewDefinition{" +
           "viewName='" + viewName + '\'' +
           ", viewNamespace='" + viewNamespace + '\'' +
           ", viewSql='" + viewSql + '\'' +
           ", lastModified=" + lastModified +
           '}';
  }
}
