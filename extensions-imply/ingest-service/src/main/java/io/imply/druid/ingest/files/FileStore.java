/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.ingest.files;

import org.apache.druid.data.input.InputSource;

import java.net.URI;
import java.util.Map;

public interface FileStore
{
  String STORE_PROPERTY_BASE = "imply.ingest.files";

  boolean fileExists(String file);
  void touchFile(String file);
  void deleteFile(String file);
  URI makeDropoffUri(String file);
  Map<String, Object> makeInputSourceMap(String file);
  InputSource makeInputSource(String file);
}
