/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.security;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.Lists;
import org.apache.druid.server.security.Action;
import org.apache.druid.server.security.Resource;
import org.apache.druid.server.security.ResourceAction;
import org.apache.druid.server.security.ResourceType;

import java.util.List;
import java.util.Map;

public class ImplyToken
{
  private final String user;
  private final long expiry;
  private final List<ResourceAction> permissions;

  @JsonCreator
  public ImplyToken(
      @JsonProperty("user") String user,
      @JsonProperty("expiry") long expiry,
      @JsonProperty("permissions") List<ResourceAction> permissions,
      @JsonProperty("appUser") Map<String, Object> appUser
  )
  {
    this.user = user;
    this.expiry = expiry;
    this.permissions = permissions;
  }

  @JsonProperty
  public List<ResourceAction> getPermissions()
  {
    return permissions;
  }

  @JsonProperty
  public String getUser()
  {
    return user;
  }

  @JsonProperty
  public long getExpiry()
  {
    return expiry;
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

    ImplyToken that = (ImplyToken) o;

    if (getExpiry() != that.getExpiry()) {
      return false;
    }
    if (getUser() != null ? !getUser().equals(that.getUser()) : that.getUser() != null) {
      return false;
    }
    return getPermissions() != null ? getPermissions().equals(that.getPermissions()) : that.getPermissions() == null;
  }

  @Override
  public int hashCode()
  {
    int result = getUser() != null ? getUser().hashCode() : 0;
    result = 31 * result + (int) (getExpiry() ^ (getExpiry() >>> 32));
    result = 31 * result + (getPermissions() != null ? getPermissions().hashCode() : 0);
    return result;
  }

  public static ImplyToken generateInternalClientToken(long expiry)
  {
    ResourceAction datasourceR = new ResourceAction(
        new Resource(".*", ResourceType.DATASOURCE),
        Action.READ
    );

    ResourceAction datasourceW = new ResourceAction(
        new Resource(".*", ResourceType.DATASOURCE),
        Action.WRITE
    );

    ResourceAction configR = new ResourceAction(
        new Resource(".*", ResourceType.CONFIG),
        Action.READ
    );

    ResourceAction configW = new ResourceAction(
        new Resource(".*", ResourceType.CONFIG),
        Action.WRITE
    );

    ResourceAction stateR = new ResourceAction(
        new Resource(".*", ResourceType.STATE),
        Action.READ
    );

    ResourceAction stateW = new ResourceAction(
        new Resource(".*", ResourceType.STATE),
        Action.WRITE
    );

    List<ResourceAction> resourceActions = Lists.newArrayList(
        datasourceR,
        datasourceW,
        configR,
        configW,
        stateR,
        stateW
    );

    return new ImplyToken("druid_internal", expiry, resourceActions, null);
  }
}
