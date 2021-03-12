/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.security.keycloak;

import org.apache.druid.java.util.common.RE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.NameValuePair;
import org.apache.http.client.HttpClient;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.message.BasicNameValuePair;
import org.keycloak.OAuth2Constants;
import org.keycloak.adapters.KeycloakDeployment;
import org.keycloak.adapters.authentication.ClientCredentialsProviderUtils;
import org.keycloak.common.util.StreamUtil;
import org.keycloak.representations.AccessTokenResponse;
import org.keycloak.util.JsonSerialization;

import javax.annotation.Nullable;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * A simple service that handles HTTP requests to grant or refresh tokens.
 * Many parts are adopted from org.keycloak.example.ProductServiceAccountServlet.
 */
public class TokenService
{
  private final KeycloakDeployment deployment;
  private final Map<String, String> reqHeaders;
  private final Map<String, String> reqParams;

  public TokenService(
      KeycloakDeployment deployment,
      Map<String, String> reqHeaders,
      Map<String, String> reqParams)
  {
    this.deployment = deployment;
    this.reqHeaders = reqHeaders != null ? reqHeaders : new HashMap<>();
    this.reqParams = reqParams != null ? reqParams : new HashMap<>();
  }

  public AccessTokenResponse grantToken()
  {
    return requestToken(null);
  }

  public AccessTokenResponse refreshToken(String refreshToken)
  {
    return requestToken(refreshToken);
  }

  private AccessTokenResponse requestToken(@Nullable String refreshToken)
  {
    HttpClient client = deployment.getClient();

    try {
      HttpPost post = new HttpPost(deployment.getTokenUrl());
      List<NameValuePair> formparams = new ArrayList<>();
      String requestedGrantType = reqParams.get(OAuth2Constants.GRANT_TYPE);
      if (requestedGrantType == null) {
        formparams.add(new BasicNameValuePair(OAuth2Constants.GRANT_TYPE, OAuth2Constants.CLIENT_CREDENTIALS));
      }
      if (refreshToken != null) {
        formparams.add(new BasicNameValuePair(OAuth2Constants.GRANT_TYPE, OAuth2Constants.REFRESH_TOKEN));
      }

      // Add client credentials according to the method configured in keycloak-client-secret.json or keycloak-client-signed-jwt.json file
      Map<String, String> reqHeadersCopy = new HashMap<>(this.reqHeaders);
      Map<String, String> reqParamsCopy = new HashMap<>(this.reqParams);
      ClientCredentialsProviderUtils.setClientCredentials(deployment, reqHeadersCopy, reqParamsCopy);
      if (refreshToken != null) {
        reqParamsCopy.put(OAuth2Constants.REFRESH_TOKEN, refreshToken);
      }
      for (Map.Entry<String, String> header : reqHeadersCopy.entrySet()) {
        post.setHeader(header.getKey(), header.getValue());
      }
      for (Map.Entry<String, String> param : reqParamsCopy.entrySet()) {
        formparams.add(new BasicNameValuePair(param.getKey(), param.getValue()));
      }

      UrlEncodedFormEntity form = new UrlEncodedFormEntity(formparams, "UTF-8");
      post.setEntity(form);

      HttpResponse response = client.execute(post);
      int status = response.getStatusLine().getStatusCode();
      HttpEntity entity = response.getEntity();
      if (status != 200) {
        String json = getContent(entity);
        String error = "Service token grant failed. Bad status: " + status + " response: " + json;
        throw new RuntimeException(error);
      } else if (entity == null) {
        throw new RuntimeException("No entity");
      } else {
        String json = getContent(entity);
        return JsonSerialization.readValue(json, AccessTokenResponse.class);
      }
    }
    catch (IOException e) {
      throw new RE(e, "Service token grant failed. IOException occured. See server.log for details.");
    }
  }

  @Nullable
  private String getContent(HttpEntity entity) throws IOException
  {
    if (entity == null) {
      return null;
    }
    InputStream is = entity.getContent();
    return StreamUtil.readString(is, StringUtils.UTF8_CHARSET);
  }
}
