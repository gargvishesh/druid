/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.druid.storage.google.output;

import com.google.common.base.Joiner;
import org.apache.druid.data.input.impl.prefetch.ObjectOpenFunction;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.storage.google.GoogleInputDataConfig;
import org.apache.druid.storage.google.GoogleStorage;
import org.apache.druid.storage.google.GoogleUtils;
import org.apache.druid.storage.remote.ChunkingStorageConnector;
import org.apache.druid.storage.remote.ChunkingStorageConnectorParameters;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Iterator;

public class GoogleStorageConnector extends ChunkingStorageConnector<GoogleInputRange>
{

  private static final String DELIM = "/";
  private static final Joiner JOINER = Joiner.on(DELIM).skipNulls();
  private static final Logger log = new Logger(GoogleStorageConnector.class);

  private final GoogleStorage storage;
  private final GoogleOutputConfig config;
  private final GoogleInputDataConfig inputDataConfig;

  public GoogleStorageConnector(
      GoogleStorage storage,
      GoogleOutputConfig config,
      GoogleInputDataConfig inputDataConfig
  )
  {
    this.storage = storage;
    this.config = config;
    this.inputDataConfig = inputDataConfig;
  }


  @Override
  public boolean pathExists(String path)
  {
    return storage.exists(config.getBucket(), objectPath(path));
  }

  @Override
  public OutputStream write(String path) throws IOException
  {
    return storage.getObjectOutputStream(config.getBucket(), path);
  }

  @Override
  public void deleteFile(String path) throws IOException
  {
    try {
      final String fullPath = objectPath(path);
      log.debug("Deleting file at bucket: [%s], path: [%s]", config.getBucket(), fullPath);

      GoogleUtils.retryGoogleCloudStorageOperation(
          () -> {
            storage.delete(config.getBucket(), fullPath);
            return null;
          }
      );
    }
    catch (Exception e) {
      log.error("Error occurred while deleting file at path [%s]. Error: [%s]", path, e.getMessage());
      throw new IOException(e);
    }
  }

  @Override
  public void deleteFiles(Iterable<String> paths) throws IOException
  {
    for (String path : paths) {
      deleteFile(objectPath(path));
    }
  }

  @Override
  public void deleteRecursively(String path) throws IOException
  {
    try {
      /*GoogleUtils.deleteObjectsInPath(
          storage,
          inputDataConfig,
          config.getBucket(),
          objectPath(config.getPrefix()),
          p -> true
      );*/
    }
    catch (Exception e) {
      throw new IOException(e);
    }
  }

  @Override
  public Iterator<String> listDir(String dirName)
  {
    /*final String fullPath = objectPath(dirName);
    Iterator<StorageObject> storageObjects = GoogleUtils.lazyFetchingStorageObjectsIterator(
        storage,
        ImmutableList.of(new CloudObjectLocation(config.getBucket(), fullPath)
                             .toUri(GoogleStorageDruidModule.SCHEME_GS)).iterator(),
        inputDataConfig.getMaxListingLength()
    );

    return Iterators.transform(
        storageObjects,
        storageObject -> {
          String[] split = storageObject.getName().split(fullPath, 2);
          if (split.length > 1) {
            return split[1];
          } else {
            return "";
          }
        }
    );*/
    return new Iterator<String>()
    {
      @Override
      public boolean hasNext()
      {
        return false;
      }

      @Override
      public String next()
      {
        return null;
      }
    };
  }

  /*@Override
  public InputStream read(String path) throws IOException
  {
    if (config.isChunkedDownloads()) {
      return super.read(path);
    }

    return storage.get(config.getBucket(), objectPath(path));
  }

  @Override
  public InputStream readRange(String path, long from, long size)
  {
    if (config.isChunkedDownloads()) {
      return super.readRange(path, from, size);
    }

    try {
      return ByteStreams.limit(storage.get(config.getBucket(), objectPath(path), from), size);
    }
    catch (IOException e) {
      throw new RuntimeException(e);
    }
  }*/

  @Override
  public ChunkingStorageConnectorParameters<GoogleInputRange> buildInputParams(String path) throws IOException
  {
    long size = storage.size(config.getBucket(), objectPath(path));
    return buildInputParams(path, 0, size);
  }

  @Override
  public ChunkingStorageConnectorParameters<GoogleInputRange> buildInputParams(String path, long from, long size)
  {
    ChunkingStorageConnectorParameters.Builder<GoogleInputRange> builder = new ChunkingStorageConnectorParameters.Builder<>();
    builder.start(from);
    builder.end(from + size);
    builder.cloudStoragePath(objectPath(path));
    builder.tempDirSupplier(config::getTempDir);
    builder.maxRetry(config.getMaxRetry());
    builder.retryCondition(GoogleUtils.GOOGLE_RETRY);
    builder.objectSupplier(((start, end) -> new GoogleInputRange(start, end - start, config.getBucket(), objectPath(path))));
    builder.objectOpenFunction(new ObjectOpenFunction<GoogleInputRange>()
    {
      @Override
      public InputStream open(GoogleInputRange googleInputRange) throws IOException
      {
        long rangeEnd = googleInputRange.getStart() + googleInputRange.getSize() - 1;
        return storage.getUsingRangeHeaders(
            googleInputRange.getBucket(),
            googleInputRange.getPath(),
            googleInputRange.getStart(),
            rangeEnd
        );
      }

      @Override
      public InputStream open(GoogleInputRange googleInputRange, long offset) throws IOException
      {
        long rangeStart = googleInputRange.getStart() + offset;
        long rangeEnd = googleInputRange.getStart() + googleInputRange.getSize() - 1;
        return storage.getUsingRangeHeaders(
            googleInputRange.getBucket(),
            googleInputRange.getPath(),
            rangeStart,
            rangeEnd
        );
      }
    });

    return builder.build();
  }

  private String objectPath(String path)
  {
    return JOINER.join(config.getPrefix(), path);
  }
}