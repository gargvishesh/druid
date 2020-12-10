/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.ingest.metadata;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.druid.server.security.Action;
import org.joda.time.DateTime;

import java.util.HashSet;
import java.util.Set;

public class Table
{
  private String name;
  private DateTime createdTime;
  private Set<Action> permissions = new HashSet<>();

  public Table(Table table)
  {
    this.name = table.getName();
    this.createdTime = table.getCreatedTime();
    this.permissions = new HashSet<>(table.getPermissions());
  }

  @JsonCreator
  public Table(
      @JsonProperty("name") String name,
      @JsonProperty("createdTime") DateTime createdTime
  )
  {
    this.name = name;
    this.createdTime = createdTime;
  }

  @JsonProperty
  public String getName()
  {
    return name;
  }

  @JsonProperty
  public DateTime getCreatedTime()
  {
    return createdTime;
  }


  @JsonProperty
  public Set<Action> getPermissions()
  {
    return permissions;
  }

  public Table addPermissions(Action permission)
  {
    this.permissions.add(permission);
    return this;
  }

  @Override
  public boolean equals(Object o)
  {
    if (o == this) {
      return true;
    }
    if (!(o instanceof Table)) {
      return false;
    }
    Table other = (Table) o;
    return this.name.equals(other.getName())
           && (this.getCreatedTime().equals(other.getCreatedTime()))
           && (this.getPermissions().equals(other.getPermissions()));
  }

  @Override
  public int hashCode()
  {
    return 31 * this.getName().hashCode() +
           this.getCreatedTime().hashCode() +
           this.getPermissions().hashCode() +
           super.hashCode();
  }

}
