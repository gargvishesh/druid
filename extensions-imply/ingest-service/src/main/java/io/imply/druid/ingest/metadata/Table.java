/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.ingest.metadata;

import org.joda.time.DateTime;

import java.util.Objects;

/**
 * Materialized object form of table data stored in the tables table of the {@link IngestServiceMetadataStore}.
 */
public class Table
{
  private String name;
  private DateTime createdTime;

  public Table(String name, DateTime createdTime)
  {
    this.name = name;
    this.createdTime = createdTime;
  }

  public String getName()
  {
    return name;
  }

  public DateTime getCreatedTime()
  {
    return createdTime;
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
    Table table = (Table) o;
    return Objects.equals(name, table.name) && Objects.equals(createdTime, table.createdTime);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(name, createdTime);
  }

  @Override
  public String toString()
  {
    return "Table{" +
           "name='" + name + '\'' +
           ", createdTime=" + createdTime +
           '}';
  }
}
