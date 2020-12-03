/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.security.keycloak;

import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.base.Preconditions;
import org.apache.druid.server.security.Access;
import org.apache.druid.server.security.Action;
import org.apache.druid.server.security.AuthenticationResult;
import org.apache.druid.server.security.Authorizer;
import org.apache.druid.server.security.Resource;

@JsonTypeName("imply-keycloak")
public class ImplyKeycloakAuthorizer implements Authorizer
{
  @Override
  public Access authorize(AuthenticationResult authenticationResult, Resource resource, Action action)
  {
    Preconditions.checkNotNull(authenticationResult, "authenticationResult");
    // TODO: do some auth when we have things
    return Access.OK;
  }
}
