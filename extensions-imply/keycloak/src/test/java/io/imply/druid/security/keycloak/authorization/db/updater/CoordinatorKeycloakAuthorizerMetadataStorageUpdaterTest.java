/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.security.keycloak.authorization.db.updater;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.smile.SmileFactory;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.metadata.MetadataStorageConnector;
import org.apache.druid.metadata.MetadataStorageTablesConfig;
import org.apache.druid.server.security.AuthorizerMapper;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

public class CoordinatorKeycloakAuthorizerMetadataStorageUpdaterTest
{
  private AuthorizerMapper authorizerMapper;
  private MetadataStorageConnector connector;
  private MetadataStorageTablesConfig connectorConfig;
  private final ObjectMapper objectMapper = new ObjectMapper(new SmileFactory());

  private CoordinatorKeycloakAuthorizerMetadataStorageUpdater storageUpdater;

  @Before
  public void setup()
  {
    authorizerMapper = Mockito.mock(AuthorizerMapper.class);
    connector = Mockito.mock(MetadataStorageConnector.class);
    connectorConfig = Mockito.mock(MetadataStorageTablesConfig.class);

    storageUpdater = new CoordinatorKeycloakAuthorizerMetadataStorageUpdater(
        authorizerMapper,
        connector,
        connectorConfig,
        objectMapper
    );
  }

  @Test (expected = ISE.class)
  public void test_start_twoTimes_throwsException()
  {
    Mockito.when(authorizerMapper.getAuthorizerMap()).thenReturn(null);
    storageUpdater.start();
    storageUpdater.start();
  }
}
