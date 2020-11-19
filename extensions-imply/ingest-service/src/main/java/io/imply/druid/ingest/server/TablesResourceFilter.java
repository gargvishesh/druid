/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.ingest.server;

import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import com.google.inject.Inject;
import com.sun.jersey.spi.container.ContainerRequest;
import org.apache.druid.server.http.security.AbstractResourceFilter;
import org.apache.druid.server.security.Access;
import org.apache.druid.server.security.AuthorizationUtils;
import org.apache.druid.server.security.AuthorizerMapper;
import org.apache.druid.server.security.ForbiddenException;
import org.apache.druid.server.security.Resource;
import org.apache.druid.server.security.ResourceAction;
import org.apache.druid.server.security.ResourceType;

// todo: rework DatasourceResourceFilter to allow overriding getRequestDatasourceName maybe?
public class TablesResourceFilter extends AbstractResourceFilter
{
  @Inject
  public TablesResourceFilter(AuthorizerMapper authorizerMapper)
  {
    super(authorizerMapper);
  }


  @Override
  public ContainerRequest filter(ContainerRequest request)
  {
    final ResourceAction resourceAction = new ResourceAction(
        new Resource(getRequestDatasourceName(request), ResourceType.DATASOURCE),
        getAction(request)
    );

    final Access authResult = AuthorizationUtils.authorizeResourceAction(
        getReq(),
        resourceAction,
        getAuthorizerMapper()
    );

    if (!authResult.isAllowed()) {
      throw new ForbiddenException(authResult.toString());
    }

    return request;
  }

  private String getRequestDatasourceName(ContainerRequest request)
  {
    final String dataSourceName = request.getPathSegments()
                                         .get(
                                             Iterables.indexOf(
                                                 request.getPathSegments(),
                                                 input -> "tables".equals(input.getPath())
                                             ) + 1
                                         ).getPath();
    Preconditions.checkNotNull(dataSourceName);
    return dataSourceName;
  }
}
