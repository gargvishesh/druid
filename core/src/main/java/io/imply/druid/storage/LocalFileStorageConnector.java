/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.storage;

import com.google.common.base.Joiner;
import org.apache.druid.java.util.common.FileUtils;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public class LocalFileStorageConnector implements StorageConnector
{

  public final String basePath;

  public static final Joiner JOINER = Joiner.on("/");

  public LocalFileStorageConnector(String basePath) throws IOException
  {
    this.basePath = basePath;
    FileUtils.mkdirp(new File(basePath));
  }

  @Override
  public boolean pathExists(String path)
  {
    return new File(objectPath(path)).exists();
  }

  @Override
  public InputStream read(String path) throws IOException
  {
    return new FileInputStream(objectPath(path));
  }

  @Override
  public OutputStream write(String path) throws IOException
  {
    File toWrite = new File(objectPath(path));
    FileUtils.mkdirp(toWrite.getParentFile());
    return new FileOutputStream(toWrite);
  }

  @Override
  public void delete(String path)
  {
    new File(objectPath(path)).delete();
  }

  private String objectPath(String path)
  {
    return JOINER.join(basePath, path);
  }

}
