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
 * Test function utils that provides sample POJOs for testing purposes.
 */
public class PolarisTestTableFunctionUtils
{
  public static PolarisTableFunctionSpec getSampleTableFunctionSpec()
  {
    return new PolarisSourceOperatorConversion.PolarisSourceFunctionSpec(
        "{\"fileList\":[\"data.json\"],\"formatSettings\":{\"format\":\"nd-json\"},\"type\":\"uploaded\"}"
    );
  }

  public static PolarisExternalTableSpec getSamplePolarisExternalTableSpec()
  {
    LocalInputSource localInputSource = new LocalInputSource(
        null,
        null,
        Collections.singletonList(
            new File("/tmp/84c90e1d-3430-4814-bfa6-d256487419c6/b90145b3-1ce8-4246-8d6a-b7f58f28076e"))
    );
    JsonInputFormat jsonInputFormat = new JsonInputFormat(null, null, null, true, null);
    return new PolarisExternalTableSpec(
        localInputSource,
        jsonInputFormat,
        null
    );
  }
}
