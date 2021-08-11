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

package org.apache.druid.sql.async;

import com.google.common.base.Strings;
import com.google.inject.Inject;
import net.jpountz.lz4.LZ4BlockInputStream;
import net.jpountz.lz4.LZ4BlockOutputStream;
import org.apache.druid.guice.ManageLifecycle;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.lifecycle.LifecycleStart;
import org.apache.druid.java.util.common.lifecycle.LifecycleStop;
import org.apache.druid.java.util.common.logger.Logger;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;
import java.util.Optional;

/**
 * TODO(gianm): Make not in-memory...
 */
@ManageLifecycle
public class LocalSqlAsyncResultManager implements SqlAsyncResultManager
{
  private static final Logger log = new Logger(LocalSqlAsyncResultManager.class);

  private final File directory;

  @Inject
  public LocalSqlAsyncResultManager(final LocalSqlAsyncResultManagerConfig config)
  {
    if (Strings.isNullOrEmpty(config.getDirectory())) {
      throw new ISE("Property 'druid.sql.asyncstorage.directory' is required");
    }

    this.directory = new File(config.getDirectory());
  }

  @LifecycleStart
  public void start()
  {
    createDirectory();
    deleteAll();
  }

  @LifecycleStop
  public void stop()
  {
    deleteAll();
  }

  @Override
  public OutputStream writeResults(final SqlAsyncQueryDetails queryDetails) throws IOException
  {
    // TODO(gianm): Limit on max result size, to avoid running out of disk
    // TODO(gianm): Tests for what error happens if the file already exists
    final FileChannel fileChannel = FileChannel.open(
        makeFile(queryDetails.getSqlQueryId()).toPath(),
        StandardOpenOption.CREATE_NEW,
        StandardOpenOption.WRITE
    );

    return new LZ4BlockOutputStream(Channels.newOutputStream(fileChannel));
  }

  @Override
  public Optional<SqlAsyncResults> readResults(final SqlAsyncQueryDetails queryDetails) throws IOException
  {
    final File file = makeFile(queryDetails.getSqlQueryId());

    if (file.exists()) {
      final FileChannel fileChannel = FileChannel.open(file.toPath(), StandardOpenOption.READ);
      return Optional.of(
          new SqlAsyncResults(
              new LZ4BlockInputStream(Channels.newInputStream(fileChannel)),
              queryDetails.getResultLength()
          )
      );
    } else {
      return Optional.empty();
    }
  }

  @Override
  public void deleteResults(final String sqlQueryId)
  {
    // TODO(gianm): Call this in a way that doesn't cause problems when two users happen to issue queries with
    //  the same manually-specified ID
    makeFile(sqlQueryId).delete();
  }

  private void createDirectory()
  {
    if (!directory.isDirectory()) {
      directory.mkdirs();

      if (!directory.isDirectory()) {
        throw new ISE(
            "Location of 'druid.sql.asyncstorage.directory' (%s) does not exist and cannot be created",
            directory
        );
      }
    }
  }

  private void deleteAll()
  {
    final File[] files = directory.listFiles();

    if (files != null) {
      for (File file : files) {
        if (!file.delete()) {
          log.warn("Could not delete async query result file: %s", file);
        }
      }
    }
  }

  private File makeFile(final String sqlQueryId)
  {
    return new File(directory, SqlAsyncUtil.safeId(sqlQueryId));
  }
}
