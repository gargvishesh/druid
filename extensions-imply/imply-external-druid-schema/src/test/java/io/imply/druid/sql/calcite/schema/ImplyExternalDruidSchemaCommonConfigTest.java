/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.sql.calcite.schema;

import org.junit.Assert;
import org.junit.Test;

public class ImplyExternalDruidSchemaCommonConfigTest
{
  private final String TABLES_SERVICE_URL = "http://test:9000/v2/tablesService"; // practically the same value as schemas url, but differentiate here.
  private final String TABLES_SCHEMAS_URL = "http://test:9000/v2/tableSchemas";

  @Test
  public void test_getTablesSchemasUrlConfig_valid()
  {

    Assert.assertEquals(TABLES_SCHEMAS_URL, new ImplyExternalDruidSchemaCommonConfig(
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        TABLES_SCHEMAS_URL,
        null
    ).getTablesSchemasUrl());

    Assert.assertEquals(TABLES_SERVICE_URL, new ImplyExternalDruidSchemaCommonConfig(
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        TABLES_SERVICE_URL,
        null,
        null
    ).getTablesSchemasUrl());

    Assert.assertEquals(TABLES_SCHEMAS_URL, new ImplyExternalDruidSchemaCommonConfig(
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        TABLES_SERVICE_URL,
        TABLES_SCHEMAS_URL,
        null
    ).getTablesSchemasUrl());

    Assert.assertNull(new ImplyExternalDruidSchemaCommonConfig(
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null
    ).getTablesSchemasUrl());
  }

  @Test
  public void test_getDeprecatedTablesServiceUrlConfig_valid()
  {
    Assert.assertEquals(TABLES_SCHEMAS_URL, new ImplyExternalDruidSchemaCommonConfig(
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        TABLES_SCHEMAS_URL,
        null
    ).getTablesServiceUrl());

    Assert.assertEquals(TABLES_SERVICE_URL, new ImplyExternalDruidSchemaCommonConfig(
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        TABLES_SERVICE_URL,
        null,
        null
    ).getTablesServiceUrl());

    Assert.assertEquals(TABLES_SERVICE_URL, new ImplyExternalDruidSchemaCommonConfig(
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        TABLES_SERVICE_URL,
        TABLES_SCHEMAS_URL,
        null
    ).getTablesServiceUrl());

    Assert.assertNull(new ImplyExternalDruidSchemaCommonConfig(
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null
    ).getTablesServiceUrl());
  }

  @Test
  public void test_oldNewSchemaUrlValues_equals()
  {
    Assert.assertEquals(
        new ImplyExternalDruidSchemaCommonConfig(
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        TABLES_SCHEMAS_URL,
        null
    ), new ImplyExternalDruidSchemaCommonConfig(
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        TABLES_SCHEMAS_URL,
        null,
        null
    ));
  }

  @Test
  public void test_oldNewSchemaUrlValues_hashCodesSame()
  {
    Assert.assertEquals(
        new ImplyExternalDruidSchemaCommonConfig(
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            TABLES_SCHEMAS_URL,
            null
        ).hashCode(),
        new ImplyExternalDruidSchemaCommonConfig(
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            TABLES_SCHEMAS_URL,
            null,
            null
        ).hashCode());
  }
}
