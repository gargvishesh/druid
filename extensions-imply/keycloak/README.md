<!--
  ~ Copyright (c) Imply Data, Inc. All rights reserved.
  ~
  ~ This software is the confidential and proprietary information
  ~ of Imply Data, Inc. You shall not disclose such Confidential
  ~ Information and shall use it only in accordance with the terms
  ~ of the license agreement you entered into with Imply.
  -->
  
# Extension for OIDC authentication and authorization

This extension provides 2 key functionalities.
- A Java Servlet Filter that performs OIDC authentication against all HTTP requests.
- An escalated HTTP client used for internal communications in a Druid cluster.
  This escalated client logs into the configured ID service on start-up and maintains its tokens to use for internal communications.

Authorization is not implemented yet.

If you want to learn more about Druid authentication and authorization in general, 
see [Druid doc](https://druid.apache.org/docs/latest/design/auth.html).
The [original PR](https://github.com/apache/druid/pull/4271) which implemented authentication and authorization
will help as well.

## Configurations

You first need to specify the authenticator and authorizer types in the `common.runtime.properties` file.

```
druid.auth.authenticatorChain=["ImplyKeycloakAuthenticator"]
druid.auth.authenticator.ImplyKeycloakAuthenticator.type=imply-keycloak
druid.auth.authenticator.ImplyKeycloakAuthenticator.authorizerName=ImplyKeycloakAuthorizer
druid.auth.authorizers=["ImplyKeycloakAuthorizer"]
druid.auth.authorizer.ImplyKeycloakAuthorizer.type=imply-keycloak
```

Then, you need to set up 2 configurations for external API calls and internal communications, in the same file.

```
# External API calls
druid.keycloak.realm=your-realm
druid.keycloak.resource=your-client
druid.keycloak.auth-server-url=http://localhost:8080/auth
druid.keycloak.bearer-only=true
...

# Internal communications
druid.escalator.type=imply-keycloak
druid.escalator.authorizerName=ImplyKeycloakAuthorizer

druid.escalator.keycloak.realm=internal-realm
druid.escalator.keycloak.resource=internal-client
druid.escalator.keycloak.auth-server-url=http://localhost:8080/auth
druid.escalator.keycloak.credentials={ ... }
druid.escalator.keycloak.bearer-only=true
...
```

For external API calls, all configurations in https://www.keycloak.org/docs/latest/securing_apps/index.html#_java_adapter_config should be supported.

For internal calls, not all configurations have been tested.
- All methods supported for `credentials` should work. See https://www.keycloak.org/docs/latest/securing_apps/index.html#_client_authentication_adapter for more details of supported methods.
- `token-minimum-time-to-live` has been tested.