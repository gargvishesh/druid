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

The first time the extension is loaded in the cluster, depending on configuration:
* it can automatically create an 'admin' role, with full permissions to all Druid operations
* prepopulate a set of role to permission mappings using a JSON serialized `Map<String, List<ResourceAction>>`
* do nothing, role to permission mapping must be defined externally (and likely with another authorizer since Keycloak will have no permission mappings and only the escalated client will be authorized)

Additional roles can be created using an HTTP JSON API.


## Configurations

You first need to specify the authenticator and authorizer types in the `common.runtime.properties` file.

#### Authenticator and Authorizer chain configuration
```
druid.auth.authenticatorChain=["ImplyKeycloakAuthenticator"]
druid.auth.authenticator.ImplyKeycloakAuthenticator.type=imply-keycloak
druid.auth.authenticator.ImplyKeycloakAuthenticator.authorizerName=ImplyKeycloakAuthorizer
druid.auth.authorizers=["ImplyKeycloakAuthorizer"]
druid.auth.authorizer.ImplyKeycloakAuthorizer.type=imply-keycloak
```

To support HTTP and JDBC OIDC authentication and role mapping, you must configure the "external" API keycloak client, specifiying the realm, clientId (resource), server location, etc.


#### External Keycloak client
This is a client configured in Keycloak for this Druid cluster. It will likely be configured as `bearer-only`, meaning that it will never directly create tokens, only process them, and any roles will be defined per this client.

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

#### Escalated Keycloak client
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

### Other config
`druid.auth.keycloak.common` is the base prefix of all other keycloak configs

|config|description|default|
| --- | --- | --- |
|`druid.auth.keycloak.common.pollingPeriod`| This controls how frequently in millis that the keycloak caches are updated. The Coordinator polls keycloak server for 'not-before' policy updates, and other node types poll the Coordinator for role to permission mapping and 'not-before' policy updates. | 60000 |
|`druid.auth.keycloak.common.maxRandomDelay`| Random delay time in millis to introduce to polling period to prevent stampede scenarios. | 6000 |
|`druid.auth.keycloak.common.cacheDirectory`| If set, the keycloak extension will store all polled caches on disk in the location specified, providing resilience to transient outages.| n/a |
|`druid.auth.keycloak.common.maxSyncRetries`| Maximum number of attempts to fetch a fresh cache before aborting (to try again the next poll period). | 10 |
|`druid.auth.keycloak.common.enableCacheNotifications`| If set to true, the Coordinator will push update notifications to all other node types whenever it detects a change in the role to permission mapping, or change in the 'not-before' policy information. If not set then these nodes will only poll the coordinator for updates. | true |
|`druid.auth.keycloak.common.cacheNotificationTimeout`| Timeout in millis for update notifications to be handled. | 5000 |
| `druid.auth.keycloak.common.notifierUpdatePeriod`| Maximum frequency at which the Coordinator will notify other node types of state updates. | 6000 |
|`druid.auth.keycloak.common.enforceNotBeforePolicies`| If set to true, token revocation will be supported because the extension will require that the 'not-before' policy information be available and also enforce that tokens were not issued (`iat`) before the timestamp (seconds since epoch) of the `not-before` policy for that client (`azp`). This information is polled from Keycloak server which requires an Imply extension to be loaded. If this information is not available (and no cache), then no external tokens can be authenticated.| true |
|`druid.auth.keycloak.common.autoPopulateAdmin`|If set to true, and `druid.auth.keycloak.common.initialRoleMappingFile` _is not_ set, this option will cause the Coordinator to create a single role named `admin` with superuser permissions to be prepopulated in the role to permission mappings. If `druid.auth.keycloak.common.initialRoleMappingFile` _is_ set, then the Coordinator will load the JSON file specified and populate the mappings provided by the file on startup.|true|
|`druid.auth.keycloak.common.initialRoleMappingFile`| This setting specifies the path to a JSON file which may be used to import role to permission mappings when the extension starts up. Note that `druid.auth.keycloak.common.autoPopulateAdmin` must also be set to true for this file to be imported. | n/a |


Example `druid.auth.keycloak.common.initialRoleMappingFile` file (located on the Coordinator filesystem):
```json
{
  "role1": [
    {"resource": {"name": "some_datasource", "type": "DATASOURCE"}, "action": "READ"}
  ],
  "role2": [
    {"resource": {"name": "other_datasource", "type": "DATASOURCE"}, "action": "READ"}
  ]
}
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