/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.security.keycloak;

import com.google.inject.Inject;
import com.sun.jersey.spi.container.ContainerRequest;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.server.http.security.AbstractResourceFilter;
import org.apache.druid.server.security.Access;
import org.apache.druid.server.security.AuthorizationUtils;
import org.apache.druid.server.security.AuthorizerMapper;
import org.apache.druid.server.security.Resource;
import org.apache.druid.server.security.ResourceAction;
import org.apache.druid.server.security.ResourceType;

import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Response;

public class KeycloakSecurityResourceFilter extends AbstractResourceFilter
{
  private static final String SECURITY_RESOURCE_NAME = "security";

  @Inject
  public KeycloakSecurityResourceFilter(
      AuthorizerMapper authorizerMapper
  )
  {
    super(authorizerMapper);
  }

  @Override
  public ContainerRequest filter(ContainerRequest request)
  {
    final ResourceAction resourceAction = new ResourceAction(
        new Resource(SECURITY_RESOURCE_NAME, ResourceType.CONFIG),
        getAction(request)
    );

    final Access authResult = AuthorizationUtils.authorizeResourceAction(
        getReq(),
        resourceAction,
        getAuthorizerMapper()
    );

    if (!authResult.isAllowed()) {
      throw new WebApplicationException(
          Response.status(Response.Status.FORBIDDEN)
                  .entity(StringUtils.format("Access-Check-Result: %s", authResult.toString()))
                  .build()
      );
    }

    return request;
  }
}
