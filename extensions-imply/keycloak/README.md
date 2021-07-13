<!--
  ~ Copyright (c) Imply Data, Inc. All rights reserved.
  ~
  ~ This software is the confidential and proprietary information
  ~ of Imply Data, Inc. You shall not disclose such Confidential
  ~ Information and shall use it only in accordance with the terms
  ~ of the license agreement you entered into with Imply.
  -->
  
# Extension for OIDC authentication and authorization

This extension provides 3 key functionalities.
- A Java Servlet Filter that performs OIDC authentication and role based authorization against all HTTP requests.
- JDBC OIDC authentication and role based authorization
- An escalated HTTP client used for internal communications in a Druid cluster.
  This escalated client logs into the configured ID service on start-up and maintains its tokens to use for internal communications, with effectively 'superuser' level authorization.

This extension supports mapping Keycloak client roles to sets of Druid permissions, storing this mapping information in the Druid metadata store. Keycloak client roles for which an authenticated user belongs to will be included in the JWT token in the `resource_access` section of the token:

```
{
  ...
  "resource_access": {
    "some-client-cluster": {
      "roles": [
        "admin"
      ]
    }
  },
  ...
}
```
If these roles have permission mappings defined in the Druid metadata store, the authenticated user will use this set of permissions for authorization.

The first time the extension is loaded in the cluster it will automatically create an 'admin' role, with full permissions to all Druid operations. Additional roles can be created using an HTTP JSON API.


## Configurations

You first need to specify the authenticator and authorizer types in the `common.runtime.properties` file.

```
druid.auth.authenticatorChain=["ImplyKeycloakAuthenticator"]
druid.auth.authenticator.ImplyKeycloakAuthenticator.type=imply-keycloak
druid.auth.authenticator.ImplyKeycloakAuthenticator.authorizerName=ImplyKeycloakAuthorizer
druid.auth.authorizers=["ImplyKeycloakAuthorizer"]
druid.auth.authorizer.ImplyKeycloakAuthorizer.type=imply-keycloak
```

To support HTTP and JDBC OIDC authentication and role mapping, you must configure the "external" API keycloak client, specifiying the realm, clientId (resource), server location, etc.


```
# External API calls
druid.keycloak.realm=your-realm
druid.keycloak.resource=your-client
druid.keycloak.auth-server-url=http://localhost:8080/auth
druid.keycloak.bearer-only=true
druid.keycloak.verify-token-audience=true
druid.keycloak.use-resource-role-mappings=true
...
```

Using keycloak for the 'escalated' internal authentication/authorization requires specifying a second internal 'escalated' config. The escalated client should be a separate service account client mapped to the base 'external' client, and tokens used by the escalated client will pick up any roles mapped in Keycloak from the standard client. However, role mapping is not necessary, as escalated client authorization is short-circuited to always have 'superuser' level permissions, since it is used strictly for internal Druid communication.

```
# Internal communications
druid.escalator.type=imply-keycloak
druid.escalator.authorizerName=ImplyKeycloakAuthorizer

druid.escalator.keycloak.realm=internal-realm
druid.escalator.keycloak.resource=internal-client
druid.escalator.keycloak.auth-server-url=http://localhost:8080/auth
druid.escalator.keycloak.credentials={"secret":"password"}
druid.escalator.keycloak.verify-token-audience=true

...
```


## API

### `GET {coordinator}/druid-ext/keycloak-security/authorization/roles`
Fetches a list of all roles which have Druid permission mappings.

Example response:
HTTP 200
```json
["admin", "some_role", "some_other_role"]
```

### `POST {coordinator}/druid-ext/keycloak-security/authorization/roles/{role}`
Create `role`.

Example response:
HTTP 200

### `GET {coordinator}/druid-ext/keycloak-security/authorization/roles/{role}`
Fetch the Druid permission mapping for `role`.

Example response:
HTTP 200
```json
{
  "name": "some_role",
  "permissions": [
    {"resource": {"name": "some_datasource", "type": "DATASOURCE"}, "action": "READ"}
  ]
}
```


### `DELETE {coordinator}/druid-ext/keycloak-security/authorization/roles/{role}`
Delete all role permission mappings for `role`.

Example response:
HTTP 200

### `POST {coordinator}/druid-ext/keycloak-security/authorization/roles/{role}/permissions`
Set the list of permissions for `role`.

Example request:
```json
[
  {"resource": {"name": "some_datasource", "type": "DATASOURCE"}, "action": "READ"}
]
```

Example response:
HTTP 200



## Additional information

If you want to learn more about Druid authentication and authorization in general,
see [Druid doc](https://druid.apache.org/docs/latest/design/auth.html).
The [original PR](https://github.com/apache/druid/pull/4271) which implemented authentication and authorization
will help as well.