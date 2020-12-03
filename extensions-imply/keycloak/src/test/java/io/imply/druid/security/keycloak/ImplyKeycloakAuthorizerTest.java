/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.security.keycloak;

import org.apache.druid.server.security.Access;
import org.apache.druid.server.security.Action;
import org.apache.druid.server.security.AuthenticationResult;
import org.apache.druid.server.security.Resource;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class ImplyKeycloakAuthorizerTest
{
  private ImplyKeycloakAuthorizer authorizer;

  @Before
  public void setup()
  {
    authorizer = new ImplyKeycloakAuthorizer();
  }

  @Test
  public void testAuthorize()
  {
    Assert.assertEquals(
        Access.OK,
        authorizer.authorize(
            new AuthenticationResult("identity", "authorizer", null, null),
            Resource.STATE_RESOURCE,
            Action.READ
        )
    );
  }
}
