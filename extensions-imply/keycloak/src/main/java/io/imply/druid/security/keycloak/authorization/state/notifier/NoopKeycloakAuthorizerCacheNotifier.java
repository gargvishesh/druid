/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.security.keycloak.authorization.state.notifier;

import java.util.function.Supplier;

public class NoopKeycloakAuthorizerCacheNotifier implements KeycloakAuthorizerCacheNotifier
{
  @Override
  public void setRoleUpdateSource(Supplier<byte[]> updateSource)
  {
    throw new UnsupportedOperationException("Non-coordinater does not support setRoleUpdateSource");
  }

  @Override
  public void scheduleRoleUpdate()
  {
    throw new UnsupportedOperationException("Non-coordinator does not support scheduleRoleUpdate");
  }

  @Override
  public void setNotBeforeUpdateSource(Supplier<byte[]> updateSource)
  {
    throw new UnsupportedOperationException("Non-coordinator does not support setNotBeforeUpdateSource");
  }

  @Override
  public void scheduleNotBeforeUpdate()
  {
    throw new UnsupportedOperationException("Non-coordinator does not support scheduleNotBeforeUpdate");
  }
}
