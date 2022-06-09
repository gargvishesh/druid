/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.storage;

import org.apache.druid.guice.annotations.UnstableApi;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;


/**
 * Low level interface for interacting with different storage providers like s3,GCS,Azure blob, local file system.
 * For adding a new implementation of this interface in  your extension extend {@link StorageConnectorProvider}.
 * For using the interface in your extension as a consumer, used JsonConfigProvider like:
 * 1. JsonConfigProvider.bind(binder, "druid,extension.custom.type", StorageConnectorProvider.class, Custom.class);
 * // bind the storage config provider
 * 2. binder.bind(Key.get(StorageConnector.class, Custom.class)).toProvider(Key.get(StorageConnectorProvider.class, Custom.class)).in(LazySingleton.class);
 * // Used Named annotations to access the storageConnector instance in your custom extension.
 * 3. @Custom StorageConnector storageConnector
 */
@UnstableApi
// TODO: add future apis
// TODO: add capability detection async skip offset mmap
// TODO: add offset
public interface StorageConnector
{

  /**
   * Check if the relative path exists in the underlying storage layer.
   * @param path
   * @return true if path exists else false.
   * @throws IOException
   */
  @SuppressWarnings("all")
  boolean pathExists(String path) throws IOException;

  /**
   * Reads the data present at the relative path from the underlying storage system.
   * The caller should take care of closing the stream when done or in case of error.
   * @param path
   * @return InputStream
   * @throws IOException if the path is not present or the unable to read the data present on the path.
   */
  InputStream read(String path) throws IOException;

  /**
   * Open an {@link OutputStream} for writing data to the relative path in the underlying storage system.
   * The caller should take care of closing the stream when done or in case of error.
   * @param path
   * @return
   * @throws IOException
   */
  OutputStream write(String path) throws IOException;

  /**
   * Delete file present at the relative path.
   * @param path
   * @throws IOException
   */
  @SuppressWarnings("all")
  void delete(String path) throws IOException;

}
