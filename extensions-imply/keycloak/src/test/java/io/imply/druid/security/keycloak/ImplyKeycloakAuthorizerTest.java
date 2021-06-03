/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.security.keycloak;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.imply.druid.security.keycloak.authorization.db.cache.KeycloakAuthorizerCacheManager;
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

import java.util.Map;
import java.util.regex.Pattern;

public class ImplyKeycloakAuthorizerTest
{
  private static final Access ACCESS_DENIED = new Access(false);
  private static final boolean CACHE_NOTIFICATIONS_ENABLED = true;
  private static final long CACHE_NOTIFICATION_TIMEOUT_MILLIS = 10L;
  private static final long NOTIFIER_UPDATE_PERIOD = 10L;

  private KeycloakAuthorizerCacheManager cacheManager;

  private ImplyKeycloakAuthorizer authorizer;

  @Before
  public void setup()
  {
    cacheManager = Mockito.mock(KeycloakAuthorizerCacheManager.class);
    authorizer = new ImplyKeycloakAuthorizer(
        CACHE_NOTIFICATIONS_ENABLED,
        CACHE_NOTIFICATION_TIMEOUT_MILLIS,
        NOTIFIER_UPDATE_PERIOD,
        cacheManager
    );
  }

  @Test
  public void test_authorize_nullContext_accessDenied()
  {
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
    Mockito.when(cacheManager.getRoleMap()).thenReturn(null);
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
  public void test_authorize_contextWithRolesNotTypeStringList_accessDenied()
  {

    Map<String, KeycloakAuthorizerRole> roleMap = ImmutableMap.of(
        "role", new KeycloakAuthorizerRole("role", ImmutableList.of(
            new KeycloakAuthorizerPermission(new ResourceAction(
                new Resource("blah", ResourceType.STATE),
                Action.READ
            ), Pattern.compile("blah"))
        ))
    );
    Mockito.when(cacheManager.getRoleMap()).thenReturn(roleMap);
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
  public void test_authorize_roleInContextNotFoundInAuthorizer_accessDenied()
  {

    Map<String, KeycloakAuthorizerRole> roleMap = ImmutableMap.of(
        "role", new KeycloakAuthorizerRole("role", ImmutableList.of(
            new KeycloakAuthorizerPermission(new ResourceAction(
                new Resource("blah", ResourceType.STATE),
                Action.READ
            ), Pattern.compile("blah"))
        ))
    );
    Mockito.when(cacheManager.getRoleMap()).thenReturn(roleMap);

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
  public void test_authorize_roleDoesNotIncludePermissionNeeded_accessDenied()
  {
    Map<String, KeycloakAuthorizerRole> roleMap = ImmutableMap.of(
        "role", new KeycloakAuthorizerRole("role", ImmutableList.of(
            new KeycloakAuthorizerPermission(new ResourceAction(
                new Resource("blah", ResourceType.STATE),
                Action.READ
            ), Pattern.compile("blah"))
        ))
    );
    Mockito.when(cacheManager.getRoleMap()).thenReturn(roleMap);
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
  public void test_authorize_roleDoesIncludePermissionNeededExactMatch_accessAllowed()
  {
    Map<String, KeycloakAuthorizerRole> roleMap = ImmutableMap.of(
        "role", new KeycloakAuthorizerRole("role", ImmutableList.of(
            new KeycloakAuthorizerPermission(new ResourceAction(
                Resource.STATE_RESOURCE,
                Action.READ
            ), Pattern.compile(Resource.STATE_RESOURCE.getName()))
        ))
    );
    Mockito.when(cacheManager.getRoleMap()).thenReturn(roleMap);
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
  public void test_authorize_roleDoesIncludePermissionNeededRegexMatch_accessAllowed()
  {
    Map<String, KeycloakAuthorizerRole> roleMap = ImmutableMap.of(
        "role", new KeycloakAuthorizerRole("role", ImmutableList.of(
            new KeycloakAuthorizerPermission(new ResourceAction(
                Resource.STATE_RESOURCE,
                Action.READ
            ), Pattern.compile(".*"))
        ))
    );
    Mockito.when(cacheManager.getRoleMap()).thenReturn(roleMap);
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
  {
    Map<String, KeycloakAuthorizerRole> roleMap = ImmutableMap.of(
        "role", new KeycloakAuthorizerRole("role", ImmutableList.of(
            new KeycloakAuthorizerPermission(new ResourceAction(
                Resource.STATE_RESOURCE,
                Action.READ
            ), Pattern.compile(".*"))
        ))
    );
    Mockito.when(cacheManager.getRoleMap()).thenReturn(roleMap);
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
