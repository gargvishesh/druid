/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.security.keycloak;

import org.apache.druid.java.util.common.StringUtils;

/**
 * Throw this for invalid resource accesses in the keycloak extension that are likely a result of user error
 * (e.g., entry not found, duplicate entries).
 */
public class KeycloakSecurityDBResourceException extends IllegalArgumentException
{
  public KeycloakSecurityDBResourceException(String formatText, Object... arguments)
  {
    super(StringUtils.nonStrictFormat(formatText, arguments));
  }

  public KeycloakSecurityDBResourceException(Throwable t, String formatText, Object... arguments)
  {
    super(StringUtils.nonStrictFormat(formatText, arguments), t);
  }
}
