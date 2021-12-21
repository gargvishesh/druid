/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.security;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.druid.server.security.AuthenticationResult;
import org.apache.druid.server.security.AuthorizationUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.util.Map;

public class ImplyEscalatorTest
{
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
  private static final String AUTHORIZER_NAME = "AUTHORIZER_NAME";
  private static final String SECRET = "SECRET";

  private ImplyEscalator target;

  @Before
  public void setup() throws NoSuchAlgorithmException, InvalidKeyException
  {
    target = new ImplyEscalator(OBJECT_MAPPER, AUTHORIZER_NAME, SECRET);
  }

  @Test
  public void testCreateEscalatedAuthenticationResultShouldReturnResultWithSuperUserPermissions()
  {
    AuthenticationResult result = target.createEscalatedAuthenticationResult();
    Map<String, Object> context = result.getContext();
    Assert.assertEquals(1, context.size());
    ImplyToken token = (ImplyToken) context.get(ImplyAuthenticator.IMPLY_TOKEN_HEADER);
    Assert.assertEquals(AuthorizationUtils.makeSuperUserPermissions(), token.getPermissions());
  }
}
