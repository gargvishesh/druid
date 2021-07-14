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

/**
 * Sends a notification to druid services, containing updated authorizer role map state.
 */
public interface KeycloakAuthorizerCacheNotifier
{
  /**
   * Set the source of updated role mapping data for the notifier. The Supplier should give the most recent view of the
   * roleMap state when get() is called.
   */
  void setRoleUpdateSource(Supplier<byte[]> updateSource);

  /**
   * Inform the notifier that an update has occurred to the role mapping. This will cause the notifier to read the
   * current state from the updateSource and send out a notification.
   */
  void scheduleRoleUpdate();

  /**
   * Set the source of updated client 'not before' token revocation data for the notifier.
   */
  void setNotBeforeUpdateSource(Supplier<byte[]> updateSource);

  /**
   * Inform the notifier that an update has occurred to the client 'not before' token revocation data.
   * This will cause the notifier to read the current state from the updateSource and send out a notification.
   */
  void scheduleNotBeforeUpdate();
}
