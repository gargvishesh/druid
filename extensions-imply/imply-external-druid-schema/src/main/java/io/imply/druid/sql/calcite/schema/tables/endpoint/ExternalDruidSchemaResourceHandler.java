/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.sql.calcite.schema.tables.endpoint;

import javax.ws.rs.core.Response;

/**
 * Handles authorizer-related API calls. Coordinator and non-coordinator methods are combined here because of an
 * inability to selectively inject jetty resources in configure(Binder binder) of the extension module based
 * on node type.
 */
public interface ExternalDruidSchemaResourceHandler
{
  Response getCachedTableSchemaMaps();

  // non-coordinator methods
  Response tableSchemaUpdateListener(byte[] serializedTableSchemaMap);
}
