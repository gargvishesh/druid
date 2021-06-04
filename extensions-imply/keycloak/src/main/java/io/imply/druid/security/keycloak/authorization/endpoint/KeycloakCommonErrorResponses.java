/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.security.keycloak.authorization.endpoint;

import com.google.common.collect.ImmutableMap;

import javax.ws.rs.core.Response;

public class KeycloakCommonErrorResponses
{
  public static Response makeResponseForAuthorizerNotFound()
  {
    return Response.status(Response.Status.BAD_REQUEST)
                   .entity(ImmutableMap.<String, Object>of(
                       "error",
                       "Keycloak authorizer does not exist."
                   ))
                   .build();
  }
}
