/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.security.keycloak;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.smile.SmileFactory;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.imply.druid.security.keycloak.authorization.db.updater.KeycloakAuthorizerMetadataStorageUpdater;
import io.imply.druid.security.keycloak.authorization.entity.KeycloakAuthorizerPermission;
import io.imply.druid.security.keycloak.authorization.entity.KeycloakAuthorizerRole;
import org.apache.druid.server.security.Access;
import org.apache.druid.server.security.Action;
import org.apache.druid.server.security.AuthenticationResult;
import org.apache.druid.server.security.Resource;
import org.apache.druid.server.security.ResourceAction;
import org.apache.druid.server.security.ResourceType;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.regex.Pattern;

public class ImplyKeycloakAuthorizerTest
{
  private static final Access ACCESS_DENIED = new Access(false);
  private KeycloakAuthorizerMetadataStorageUpdater storageUpdater;
  private final ObjectMapper objectMapper = new ObjectMapper(new SmileFactory());

  private ImplyKeycloakAuthorizer authorizer;

  @Before
  public void setup()
  {
    storageUpdater = Mockito.mock(KeycloakAuthorizerMetadataStorageUpdater.class);
    authorizer = new ImplyKeycloakAuthorizer(storageUpdater, objectMapper);
  }

  @Test
  public void test_authorize_nullContext_accessDenied()
  {
    Mockito.when(storageUpdater.getCurrentRoleMapBytes()).thenReturn(null);
    Assert.assertEquals(
        ACCESS_DENIED.isAllowed(),
        authorizer.authorize(
            new AuthenticationResult("identity", "authorizer", null, null),
            Resource.STATE_RESOURCE,
            Action.READ
        ).isAllowed()
    );
  }

  @Test
  public void test_authorize_emptyContext_accessDenied()
  {
    Mockito.when(storageUpdater.getCurrentRoleMapBytes()).thenReturn(null);
    Assert.assertEquals(
        ACCESS_DENIED.isAllowed(),
        authorizer.authorize(
            new AuthenticationResult("identity", "authorizer", null, ImmutableMap.of()),
            Resource.STATE_RESOURCE,
            Action.READ
        ).isAllowed()
    );
  }

  @Test
  public void test_authorize_contextWithRolesNotTypeStringList_accessDenied() throws JsonProcessingException
  {
    byte[] roleMapBytes = objectMapper.writeValueAsBytes(ImmutableMap.of(
        "role", new KeycloakAuthorizerRole("role", ImmutableList.of(
            new KeycloakAuthorizerPermission(new ResourceAction(
                new Resource("blah", ResourceType.STATE),
                Action.READ
            ), Pattern.compile("blah"))
        ))
    ));
    Mockito.when(storageUpdater.getCurrentRoleMapBytes()).thenReturn(roleMapBytes);
    Assert.assertEquals(
        ACCESS_DENIED.isAllowed(),
        authorizer.authorize(
            new AuthenticationResult(
                "identity",
                "authorizer",
                null,
                ImmutableMap.of(KeycloakAuthUtils.AUTHENTICATED_ROLES_CONTEXT_KEY, ImmutableList.of(2))
            ),
            Resource.STATE_RESOURCE,
            Action.READ
        ).isAllowed()
    );
  }

  @Test
  public void test_authorize_roleInContextNotFoundInAuthorizer_accessDenied() throws JsonProcessingException
  {
    byte[] roleMapBytes = objectMapper.writeValueAsBytes(ImmutableMap.of(
        "role", new KeycloakAuthorizerRole("role", ImmutableList.of(
            new KeycloakAuthorizerPermission(new ResourceAction(
                new Resource("blah", ResourceType.STATE),
                Action.READ
            ), Pattern.compile("blah"))
        ))
    ));
    Mockito.when(storageUpdater.getCurrentRoleMapBytes()).thenReturn(roleMapBytes);
    Assert.assertEquals(
        ACCESS_DENIED.isAllowed(),
        authorizer.authorize(
            new AuthenticationResult(
                "identity",
                "authorizer",
                null,
                ImmutableMap.of(KeycloakAuthUtils.AUTHENTICATED_ROLES_CONTEXT_KEY, ImmutableList.of("superRole"))
            ),
            Resource.STATE_RESOURCE,
            Action.READ
        ).isAllowed()
    );
  }

  @Test
  public void test_authorize_roleDoesNotIncludePermissionNeeded_accessDenied() throws JsonProcessingException
  {
    byte[] roleMapBytes = objectMapper.writeValueAsBytes(ImmutableMap.of(
        "role", new KeycloakAuthorizerRole("role", ImmutableList.of(
            new KeycloakAuthorizerPermission(new ResourceAction(
                new Resource("blah", ResourceType.STATE),
                Action.READ
            ), Pattern.compile("blah"))
        ))
    ));
    Mockito.when(storageUpdater.getCurrentRoleMapBytes()).thenReturn(roleMapBytes);
    Assert.assertEquals(
        ACCESS_DENIED.isAllowed(),
        authorizer.authorize(
            new AuthenticationResult(
                "identity",
                "authorizer",
                null,
                ImmutableMap.of(KeycloakAuthUtils.AUTHENTICATED_ROLES_CONTEXT_KEY, ImmutableList.of("role"))
            ),
            Resource.STATE_RESOURCE,
            Action.READ
        ).isAllowed()
    );
  }

  @Test
  public void test_authorize_roleDoesIncludePermissionNeededExactMatch_accessAllowed() throws JsonProcessingException
  {
    byte[] roleMapBytes = objectMapper.writeValueAsBytes(ImmutableMap.of(
        "role", new KeycloakAuthorizerRole("role", ImmutableList.of(
            new KeycloakAuthorizerPermission(new ResourceAction(
                Resource.STATE_RESOURCE,
                Action.READ
            ), Pattern.compile(Resource.STATE_RESOURCE.getName()))
        ))
    ));
    Mockito.when(storageUpdater.getCurrentRoleMapBytes()).thenReturn(roleMapBytes);
    Assert.assertEquals(
        Access.OK.isAllowed(),
        authorizer.authorize(
            new AuthenticationResult(
                "identity",
                "authorizer",
                null,
                ImmutableMap.of(KeycloakAuthUtils.AUTHENTICATED_ROLES_CONTEXT_KEY, ImmutableList.of("role"))
            ),
            Resource.STATE_RESOURCE,
            Action.READ
        ).isAllowed()
    );
  }

  @Test
  public void test_authorize_roleDoesIncludePermissionNeededRegexMatch_accessAllowed() throws JsonProcessingException
  {
    byte[] roleMapBytes = objectMapper.writeValueAsBytes(ImmutableMap.of(
        "role", new KeycloakAuthorizerRole("role", ImmutableList.of(
            new KeycloakAuthorizerPermission(new ResourceAction(
                Resource.STATE_RESOURCE,
                Action.READ
            ), Pattern.compile(".*"))
        ))
    ));
    Mockito.when(storageUpdater.getCurrentRoleMapBytes()).thenReturn(roleMapBytes);
    Assert.assertEquals(
        Access.OK.isAllowed(),
        authorizer.authorize(
            new AuthenticationResult(
                "identity",
                "authorizer",
                null,
                ImmutableMap.of(KeycloakAuthUtils.AUTHENTICATED_ROLES_CONTEXT_KEY, ImmutableList.of("role"))
            ),
            Resource.STATE_RESOURCE,
            Action.READ
        ).isAllowed()
    );
  }

  @Test
  public void test_authorize_roleDoesIncludePermissionNeededRegexMatchAndNonStringRoleInContext_accessAllowed()
      throws JsonProcessingException
  {
    byte[] roleMapBytes = objectMapper.writeValueAsBytes(ImmutableMap.of(
        "role", new KeycloakAuthorizerRole("role", ImmutableList.of(
            new KeycloakAuthorizerPermission(new ResourceAction(
                Resource.STATE_RESOURCE,
                Action.READ
            ), Pattern.compile(".*"))
        ))
    ));
    Mockito.when(storageUpdater.getCurrentRoleMapBytes()).thenReturn(roleMapBytes);
    Assert.assertEquals(
        Access.OK.isAllowed(),
        authorizer.authorize(
            new AuthenticationResult(
                "identity",
                "authorizer",
                null,
                ImmutableMap.of(KeycloakAuthUtils.AUTHENTICATED_ROLES_CONTEXT_KEY, ImmutableList.of(2, "role"))
            ),
            Resource.STATE_RESOURCE,
            Action.READ
        ).isAllowed()
    );
  }
}
