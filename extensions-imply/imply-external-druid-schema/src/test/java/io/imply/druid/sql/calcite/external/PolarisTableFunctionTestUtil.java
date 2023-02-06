/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.sql.calcite.external;

import org.apache.druid.data.input.impl.JsonInputFormat;
import org.apache.druid.data.input.impl.LocalInputSource;

import java.io.File;
import java.util.Collections;

/**
 * Test utils related to table functions.
 */
public class PolarisTableFunctionTestUtil
{
  public static final PolarisTableFunctionSpec TEST_TABLE_FUNCTION_SPEC = new PolarisSourceOperatorConversion.PolarisSourceFunctionSpec(
      "{\"fileList\":[\"data.json\"],\"formatSettings\":{\"format\":\"nd-json\"},\"type\":\"uploaded\"}"
  );
  public static final PolarisExternalTableSpec TEST_EXTERNAL_TABLE_SPEC = new PolarisExternalTableSpec(
      new LocalInputSource(
          null,
          null,
          Collections.singletonList(
              new File("/tmp/84c90e1d-3430-4814-bfa6-d256487419c6/b90145b3-1ce8-4246-8d6a-b7f58f28076e"))
      ),
      new JsonInputFormat(null, null, null, true, null),
      null
  );
}
