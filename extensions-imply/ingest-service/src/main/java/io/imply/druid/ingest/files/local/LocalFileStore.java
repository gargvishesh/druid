/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.ingest.files.local;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.Files;
import com.google.inject.Inject;
import io.imply.druid.ingest.config.IngestServiceTenantConfig;
import io.imply.druid.ingest.files.BaseFileStore;
import org.apache.druid.guice.annotations.Json;
import org.apache.druid.java.util.common.StringUtils;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.Map;

// don't really use this, is for testing against local disk
public class LocalFileStore extends BaseFileStore
{
  private final IngestServiceTenantConfig serviceConfig;
  private final LocalFileStoreConfig fileConfig;

  @Inject
  public LocalFileStore(
      IngestServiceTenantConfig serviceConfig,
      LocalFileStoreConfig fileConfig,
      @Json ObjectMapper jsonMapper
  )
  {
    super(jsonMapper);
    this.serviceConfig = serviceConfig;
    this.fileConfig = fileConfig;
  }

  @Override
  public void touchFile(String file)
  {
    try {
      Files.touch(makeFile(file));
    }
    catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public boolean fileExists(String file)
  {
    return makeFile(file).exists();
  }

  @Override
  public void deleteFile(String file)
  {
    File toDelete = makeFile(file);
    toDelete.delete();
  }

  @Override
  public URI makeDropoffUri(String file)
  {
    return makeFile(file).toURI();
  }

  @Override
  public Map<String, Object> makeInputSourceMap(String file)
  {
    return ImmutableMap.of(
        "type", LocalFileStoreModule.TYPE,
        "files", ImmutableList.of(makeFullPath(file))
    );
  }

  private File makeFile(String name)
  {
    return new File(makeFullPath(name));
  }

  private String makeFullPath(String name)
  {
    return StringUtils.format(
        "%s%s",
        fileConfig.getBaseDir(),
        getSubPath(name)
    );
  }

  private String getSubPath(String name)
  {
    return StringUtils.format("/%s/%s/files/%s", serviceConfig.getAccountId(), serviceConfig.getClusterId(), name);
  }
}
