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

public class KeycloakSecurityBadRequestException extends RuntimeException
{
  public KeycloakSecurityBadRequestException(String formatText, Object... arguments)
  {
    super(StringUtils.nonStrictFormat(formatText, arguments));
  }

  public KeycloakSecurityBadRequestException(Throwable t, String formatText, Object... arguments)
  {
    super(StringUtils.nonStrictFormat(formatText, arguments), t);
  }
}
