/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.sql.async.result;

import com.google.common.base.Strings;
import com.google.inject.Inject;
import io.imply.druid.sql.async.SqlAsyncUtil;
import io.imply.druid.sql.async.guice.SqlAsyncCoreModule;
import io.imply.druid.sql.async.query.SqlAsyncQueryDetails;
import net.jpountz.lz4.LZ4BlockInputStream;
import net.jpountz.lz4.LZ4BlockOutputStream;
import org.apache.druid.guice.ManageLifecycle;
import org.apache.druid.java.util.common.FileUtils;
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
import java.util.Collection;
import java.util.HashSet;
import java.util.Optional;

@ManageLifecycle
public class LocalSqlAsyncResultManager implements SqlAsyncResultManager
{
  public static final String LOCAL_RESULT_MANAGER_TYPE = "local";
  public static final String BASE_LOCAL_STORAGE_CONFIG_KEY = String.join(
      ".",
      SqlAsyncCoreModule.BASE_STORAGE_CONFIG_KEY,
      LOCAL_RESULT_MANAGER_TYPE
  );
  public static final String LOCAL_STORAGE_DIRECTORY_CONFIG_KEY = String.join(
      ".",
      BASE_LOCAL_STORAGE_CONFIG_KEY,
      "directory"
  );

  private static final Logger log = new Logger(LocalSqlAsyncResultManager.class);
  private final File directory;

  @Inject
  public LocalSqlAsyncResultManager(final LocalSqlAsyncResultManagerConfig config)
  {
    if (Strings.isNullOrEmpty(config.getDirectory())) {
      throw new ISE(
          "Property '%s' is required",
          LOCAL_STORAGE_DIRECTORY_CONFIG_KEY
      );
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
    // Should have a limit on max result size, to avoid running out of disk.
    // Should have tests for what error happens if the file already exists.
    final FileChannel fileChannel = FileChannel.open(
        makeFile(queryDetails.getAsyncResultId()).toPath(),
        StandardOpenOption.CREATE_NEW,
        StandardOpenOption.WRITE
    );

    return new LZ4BlockOutputStream(Channels.newOutputStream(fileChannel));
  }

  @Override
  public Optional<SqlAsyncResults> readResults(final SqlAsyncQueryDetails queryDetails) throws IOException
  {
    final File file = makeFile(queryDetails.getAsyncResultId());

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
  public boolean deleteResults(final String asyncResultId)
  {
    File result = makeFile(asyncResultId);
    if (!result.exists()) {
      return true;
    } else {
      return result.delete();
    }
  }

  @Override
  public Collection<String> getAllAsyncResultIds()
  {
    final File[] files = directory.listFiles();
    final Collection<String> results = new HashSet<>();
    if (files != null) {
      for (File file : files) {
        results.add(file.getName());
      }
    }
    return results;
  }

  @Override
  public long getResultSize(String asyncResultId) throws IOException
  {
    File result = makeFile(asyncResultId);
    if (!result.exists()) {
      throw new IOException("File does not exist");
    } else {
      return result.length();
    }
  }

  private void createDirectory()
  {
    if (!directory.isDirectory()) {
      try {
        FileUtils.mkdirp(directory);
      }
      catch (IOException e) {
        throw new IllegalStateException(e);
      }

      if (!directory.isDirectory()) {
        throw new ISE(
            "Location of '%s' (%s) does not exist and cannot be created",
            LOCAL_STORAGE_DIRECTORY_CONFIG_KEY,
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

  private File makeFile(final String asyncResultId)
  {
    return new File(directory, SqlAsyncUtil.safeId(asyncResultId));
  }
}
