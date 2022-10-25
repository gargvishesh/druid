/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.security.keycloak;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.server.security.AuthConfig;
import org.apache.druid.server.security.AuthenticationResult;
import org.keycloak.KeycloakSecurityContext;
import org.keycloak.adapters.AdapterDeploymentContext;
import org.keycloak.adapters.AuthenticatedActionsHandler;
import org.keycloak.adapters.KeycloakConfigResolver;
import org.keycloak.adapters.KeycloakDeployment;
import org.keycloak.adapters.NodesRegistrationManagement;
import org.keycloak.adapters.OidcKeycloakAccount;
import org.keycloak.adapters.PreAuthActionsHandler;
import org.keycloak.adapters.servlet.FilterRequestAuthenticator;
import org.keycloak.adapters.servlet.KeycloakOIDCFilter;
import org.keycloak.adapters.servlet.OIDCFilterSessionStore;
import org.keycloak.adapters.servlet.OIDCServletHttpFacade;
import org.keycloak.adapters.spi.AuthChallenge;
import org.keycloak.adapters.spi.AuthOutcome;
import org.keycloak.adapters.spi.InMemorySessionIdMapper;
import org.keycloak.adapters.spi.KeycloakAccount;
import org.keycloak.adapters.spi.SessionIdMapper;
import org.keycloak.adapters.spi.UserSessionManagement;
import org.keycloak.representations.AccessToken;

import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletRequestWrapper;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpSession;
import java.io.IOException;
import java.util.List;

/**
 * This class is adopted from org.keycloak.adapters.servlet.KeycloakOIDCFilter and modified to follow the contract
 * of Druid authenticator. See {@link #doFilter} for more details.
 */
public class DruidKeycloakOIDCFilter implements Filter
{
  private static final Logger LOG = new Logger(KeycloakOIDCFilter.class);

  private final SessionIdMapper idMapper = new InMemorySessionIdMapper();
  private final DruidKeycloakConfigResolver configResolver;
  private final String authenticatorName;
  private final String authorizerName;
  private final AccessTokenValidator tokenValidator;

  private AdapterDeploymentContext deploymentContext;
  private NodesRegistrationManagement nodesRegistrationManagement;

  /**
   * Constructor that can be used to define a {@code KeycloakConfigResolver} that will be used at initialization to
   * provide the {@code KeycloakDeployment}.
   */
  public DruidKeycloakOIDCFilter(
      DruidKeycloakConfigResolver configResolver,
      String authenticatorName,
      String authorizerName
  )
  {
    this.configResolver = Preconditions.checkNotNull(configResolver, "configResolver");
    this.authenticatorName = Preconditions.checkNotNull(authenticatorName, "authenticatorName");
    this.authorizerName = Preconditions.checkNotNull(authorizerName, "authorizerName");
    this.tokenValidator = new AccessTokenValidator(authenticatorName, authorizerName, configResolver);
  }

  @VisibleForTesting
  String getAuthenticatorName()
  {
    return authenticatorName;
  }

  @VisibleForTesting
  String getAuthorizerName()
  {
    return authorizerName;
  }

  @VisibleForTesting
  KeycloakConfigResolver getConfigResolver()
  {
    return configResolver;
  }

  @Override
  public void init(final FilterConfig filterConfig)
  {
    // The original implementation has an if clause here to handle the case where configResolver is null.
    // This part was removed because configResolver can't be null in Druid.
    deploymentContext = new AdapterDeploymentContext(configResolver);
    LOG.info("Using %s to resolve Keycloak configuration on a per-request basis.", configResolver.getClass());

    filterConfig.getServletContext().setAttribute(AdapterDeploymentContext.class.getName(), deploymentContext);
    nodesRegistrationManagement = new NodesRegistrationManagement();
  }

  private boolean doesRequestHaveBearerAuth(HttpServletRequest httpReq)
  {
    String authHeader = httpReq.getHeader("Authorization");

    if (authHeader == null) {
      return false;
    }

    if (authHeader.length() < 7) {
      return false;
    }

    if (!"Bearer ".equals(authHeader.substring(0, 7))) {
      return false;
    }

    return true;
  }

  @Override
  public void doFilter(ServletRequest req, ServletResponse res, FilterChain chain) throws IOException, ServletException
  {
    HttpServletRequest request = (HttpServletRequest) req;
    HttpServletResponse response = (HttpServletResponse) res;

    if (!doesRequestHaveBearerAuth(request)) {
      // Request didn't have HTTP Bearer token, move on to the next filter
      chain.doFilter(request, response);
      return;
    }

    OIDCServletHttpFacade facade = new OIDCServletHttpFacade(request, response);
    KeycloakDeployment deployment = deploymentContext.resolveDeployment(facade);
    if (deployment == null || !deployment.isConfigured()) {
      response.sendError(HttpServletResponse.SC_FORBIDDEN);
      LOG.warn("deployment not configured");
      return;
    }

    PreAuthActionsHandler preActions = getPreAuthActionsHandler(facade);

    if (preActions.handleRequest()) {
      return;
    }

    nodesRegistrationManagement.tryRegister(deployment);
    OIDCFilterSessionStore tokenStore = getOIDCFilterSessionStore(request, facade, 100000, deployment);
    tokenStore.checkCurrentToken();

    FilterRequestAuthenticator authenticator = getFilterRequestAuthenticator(deployment, tokenStore, facade, request, 8443);
    AuthOutcome outcome = authenticator.authenticate();
    if (outcome == AuthOutcome.AUTHENTICATED) {
      LOG.debug("AUTHENTICATED");
      if (facade.isEnded()) {
        return;
      }
      AuthenticatedActionsHandler actions = getAuthenticatedActionsHandler(deployment, facade);
      if (actions.handledRequest()) {
        return;
      } else {
        HttpServletRequestWrapper wrapper = tokenStore.buildWrapper();

        // This is the only different part from the original implementation in doFilter().
        // Here, we set the authenticationResult in the request before calling the rest of chains,
        // so that the authorizer can do its job properly.
        // ------- new part start -------
        final AccessToken token = getAccessToken(request);
        if (token != null) {
          final AuthenticationResult authenticationResult = tokenValidator.authenticateToken(token, deployment);
          if (authenticationResult == null) {
            response.sendError(HttpServletResponse.SC_UNAUTHORIZED);
            return;
          }
          wrapper.setAttribute(AuthConfig.DRUID_AUTHENTICATION_RESULT, authenticationResult);
        }
        // ------- new part end -------
        chain.doFilter(wrapper, res);
        return;
      }
    }
    AuthChallenge challenge = authenticator.getChallenge();
    if (challenge != null) {
      LOG.debug("challenge");
      challenge.challenge(facade);
      return;
    }
    response.sendError(HttpServletResponse.SC_UNAUTHORIZED);
  }

  @VisibleForTesting
  PreAuthActionsHandler getPreAuthActionsHandler(OIDCServletHttpFacade facade)
  {
    return new PreAuthActionsHandler(new UserSessionManagement()
    {
      @Override
      public void logoutAll()
      {
        idMapper.clear();
      }

      @Override
      public void logoutHttpSessions(List<String> ids)
      {
        LOG.debug("**************** logoutHttpSessions");
        for (String id : ids) {
          LOG.debug("removed idMapper: " + id);
          idMapper.removeSession(id);
        }

      }
    }, deploymentContext, facade);
  }

  @VisibleForTesting
  OIDCFilterSessionStore getOIDCFilterSessionStore(
      HttpServletRequest request,
      OIDCServletHttpFacade facade,
      int maxBuffer,
      KeycloakDeployment deployment)
  {
    return new OIDCFilterSessionStore(request, facade, maxBuffer, deployment, idMapper);
  }

  @VisibleForTesting
  FilterRequestAuthenticator getFilterRequestAuthenticator(
      KeycloakDeployment deployment,
      OIDCFilterSessionStore tokenStore,
      OIDCServletHttpFacade facade,
      HttpServletRequest request,
      int sslRedirectPort

  )
  {
    return new FilterRequestAuthenticator(deployment, tokenStore, facade, request, sslRedirectPort);
  }

  @VisibleForTesting
  AuthenticatedActionsHandler getAuthenticatedActionsHandler(
      KeycloakDeployment deployment,
      OIDCServletHttpFacade facade
  )
  {
    return new AuthenticatedActionsHandler(deployment, facade);
  }

  private AccessToken getAccessToken(HttpServletRequest request)
  {
    HttpSession session = request.getSession(false);
    KeycloakAccount account = null;
    if (session != null) {
      account = (KeycloakAccount) session.getAttribute(KeycloakAccount.class.getName());
      if (account == null) {
        account = (KeycloakAccount) request.getAttribute(KeycloakAccount.class.getName());
      }
    }
    if (account == null) {
      account = (KeycloakAccount) request.getAttribute(KeycloakAccount.class.getName());
    }
    if (account instanceof OidcKeycloakAccount) {
      final OidcKeycloakAccount oidcKeycloakAccount = (OidcKeycloakAccount) account;
      final KeycloakSecurityContext securityContext = oidcKeycloakAccount.getKeycloakSecurityContext();
      return securityContext.getToken();
    }
    return null;
  }

  @Override
  public void destroy()
  {

  }
}