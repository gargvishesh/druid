/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.talaria.indexing.externalsink;

import com.google.common.base.Preconditions;
import com.google.inject.Inject;
import org.apache.druid.java.util.common.FileUtils;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.UOE;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import java.util.zip.GZIPOutputStream;

public class LocalTalariaExternalSink implements TalariaExternalSink
{
  // TODO(gianm): handle partitions > MAX_PARTITION_NUMBER
  // TODO(gianm): this exists because higher numbers would break lexicographic sortability of URIs, which is relied upon
  //   in the SortedSet<URI> return value from TalariaExternalSinkWorkerFactory
  private static final int MAX_PARTITION_NUMBER = 999_999;
  private static final String FILE_PATTERN = "%06d.json.gz";

  public static final String TYPE = "local";

  private final File baseDirectory;

  @Inject
  public LocalTalariaExternalSink(final LocalTalariaExternalSinkConfig config)
  {
    this.baseDirectory = Preconditions.checkNotNull(config.getDirectory(), "Missing 'directory'");
  }

  @Override
  public TalariaExternalSinkStream open(final String queryId, final int partitionNumber) throws IOException
  {
    if (partitionNumber > MAX_PARTITION_NUMBER) {
      throw new UOE("Cannot handle more than [%,d] partitions", MAX_PARTITION_NUMBER);
    }

    final File queryDirectory = new File(baseDirectory, Preconditions.checkNotNull(queryId, "queryId"));

    FileUtils.mkdirp(queryDirectory);

    final File file = new File(queryDirectory, StringUtils.format(FILE_PATTERN, partitionNumber));

    // Open with CREATE_NEW to ensure we don't overwrite an already-existing file.
    final OutputStream outputStream = Files.newOutputStream(
        file.toPath(),
        StandardOpenOption.CREATE_NEW,
        StandardOpenOption.WRITE
    );

    return new TalariaExternalSinkStream(file.toURI(), new GZIPOutputStream(outputStream));
  }
}
