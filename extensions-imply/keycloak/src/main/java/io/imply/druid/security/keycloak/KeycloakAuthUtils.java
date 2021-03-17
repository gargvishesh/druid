package io.imply.druid.security.keycloak;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Predicate;
import io.imply.druid.security.keycloak.authorization.entity.KeycloakAuthorizerRole;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.RetryUtils;
import org.apache.druid.java.util.common.logger.Logger;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class KeycloakAuthUtils
{
  private static final Logger log = new Logger(KeycloakAuthUtils.class);

  public static final String ADMIN_NAME = "admin";
  public static final String KEYCLOAK_AUTHORIZER_NAME = "keycloak-authorizer";

  public static final Predicate<Throwable> SHOULD_RETRY_INIT =
      (throwable) -> throwable instanceof KeycloakSecurityDBResourceException;

  public static final int MAX_INIT_RETRIES = 2;

  public static final TypeReference<Map<String, KeycloakAuthorizerRole>> AUTHORIZER_ROLE_MAP_TYPE_REFERENCE =
      new TypeReference<Map<String, KeycloakAuthorizerRole>>()
      {
      };

  public static Map<String, KeycloakAuthorizerRole> deserializeAuthorizerRoleMap(
      ObjectMapper objectMapper,
      byte[] roleMapBytes
  )
  {
    Map<String, KeycloakAuthorizerRole> roleMap;
    if (roleMapBytes == null) {
      roleMap = new HashMap<>();
    } else {
      try {
        roleMap = objectMapper.readValue(roleMapBytes, KeycloakAuthUtils.AUTHORIZER_ROLE_MAP_TYPE_REFERENCE);
      }
      catch (IOException ioe) {
        throw new RuntimeException("Couldn't deserialize authorizer roleMap!", ioe);
      }
    }
    return roleMap;
  }

  public static byte[] serializeAuthorizerRoleMap(ObjectMapper objectMapper, Map<String, KeycloakAuthorizerRole> roleMap)
  {
    try {
      return objectMapper.writeValueAsBytes(roleMap);
    }
    catch (IOException ioe) {
      throw new ISE(ioe, "Couldn't serialize authorizer roleMap!");
    }
  }

  public static void maybeInitialize(final RetryUtils.Task<?> task)
  {
    try {
      RetryUtils.retry(task, SHOULD_RETRY_INIT, MAX_INIT_RETRIES);
    }
    catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
