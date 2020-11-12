/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc.
 */

package io.imply.security;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonTypeName;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.server.security.Access;
import org.apache.druid.server.security.Action;
import org.apache.druid.server.security.AuthenticationResult;
import org.apache.druid.server.security.Authorizer;
import org.apache.druid.server.security.Resource;
import org.apache.druid.server.security.ResourceAction;

import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@JsonTypeName("imply")
public class ImplyAuthorizer implements Authorizer
{
  private static final Logger log = new Logger(ImplyAuthorizer.class);

  @JsonCreator
  public ImplyAuthorizer()
  {
  }

  @Override
  public Access authorize(
      AuthenticationResult authenticationResult,
      Resource resource,
      Action action
  )
  {
    try {
      Map<String, Object> authenticationContext = authenticationResult.getContext();
      if (authenticationContext == null) {
        return new Access(false);
      }

      final ImplyToken implyToken = (ImplyToken) authenticationContext.get(ImplyAuthenticator.IMPLY_TOKEN_HEADER);
      if (implyToken == null) {
        return new Access(false);
      }

      long curTime = System.currentTimeMillis();
      if (curTime > implyToken.getExpiry()) {
        return new Access(false);
      }

      for (ResourceAction permission : implyToken.getPermissions()) {
        if (permissionCheck(resource, action, permission)) {
          return new Access(true);
        }
      }
      return new Access(false);
    }
    catch (Exception ex) {
      return new Access(false);
    }
  }

  private boolean permissionCheck(Resource resource, Action action, ResourceAction permissionResourceAction)
  {
    if (action != permissionResourceAction.getAction()) {
      return false;
    }

    Resource permissionResource = permissionResourceAction.getResource();
    if (permissionResource.getType() != resource.getType()) {
      return false;
    }

    String permissionResourceName = permissionResource.getName();
    Pattern resourceNamePattern = Pattern.compile(permissionResourceName);
    Matcher resourceNameMatcher = resourceNamePattern.matcher(resource.getName());
    return resourceNameMatcher.matches();
  }
}
