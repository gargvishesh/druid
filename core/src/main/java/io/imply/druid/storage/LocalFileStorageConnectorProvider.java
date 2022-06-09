/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.storage;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import org.apache.druid.java.util.common.ISE;

import java.io.IOException;

@JsonTypeName("local")
public class LocalFileStorageConnectorProvider implements StorageConnectorProvider
{
  @JsonProperty
  String basePath;

  @JsonCreator
  public LocalFileStorageConnectorProvider(String basePath)
  {
    this.basePath = basePath;
  }

  @Override
  public StorageConnector get()
  {
    try {
      return new LocalFileStorageConnector(basePath);
    }
    catch (IOException e) {
      throw new ISE(e,
                    "Unable to create storageConnector[%s] for basepath[%s]",
                    LocalFileStorageConnector.class.getSimpleName(),
                    basePath);
    }
  }
}
