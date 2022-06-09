/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.talaria.guice;

import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.TypeLiteral;
import org.apache.druid.discovery.NodeRole;
import org.apache.druid.guice.annotations.Self;
import org.apache.druid.java.util.common.logger.Logger;

import java.util.Collections;
import java.util.Set;

public class TalariaModules
{
  private static final Logger log = new Logger(TalariaModules.class);

  static Set<NodeRole> getNodeRoles(final Injector injector)
  {
    try {
      return injector.getInstance(Key.get(new TypeLiteral<Set<NodeRole>>() {}, Self.class));
    }
    catch (Exception e) {
      log.noStackTrace().warn(e, "Cannot determine node roles.");
      return Collections.emptySet();
    }
  }
}
