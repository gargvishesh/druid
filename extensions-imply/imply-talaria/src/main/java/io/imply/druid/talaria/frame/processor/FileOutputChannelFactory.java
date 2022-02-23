/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.talaria.frame.processor;

import com.google.common.base.Suppliers;
import io.imply.druid.talaria.frame.channel.ReadableFileFrameChannel;
import io.imply.druid.talaria.frame.channel.ReadableFrameChannel;
import io.imply.druid.talaria.frame.channel.WritableStreamFrameChannel;
import io.imply.druid.talaria.frame.file.FrameFile;
import io.imply.druid.talaria.frame.file.FrameFileWriter;
import io.imply.druid.talaria.frame.write.ArenaMemoryAllocator;
import org.apache.druid.java.util.common.FileUtils;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import java.util.UUID;
import java.util.function.Supplier;

public class FileOutputChannelFactory implements OutputChannelFactory
{
  private final File fileChannelsDirectory;
  private final int frameSize;

  public FileOutputChannelFactory(final File fileChannelsDirectory, final int frameSize)
  {
    this.fileChannelsDirectory = fileChannelsDirectory;
    this.frameSize = frameSize;
  }

  @Override
  public OutputChannel openChannel(int partitionNumber) throws IOException
  {
    FileUtils.mkdirp(fileChannelsDirectory);

    final File file = new File(fileChannelsDirectory, UUID.randomUUID().toString());

    final WritableStreamFrameChannel writableChannel =
        new WritableStreamFrameChannel(
            FrameFileWriter.open(
                Files.newByteChannel(
                    file.toPath(),
                    StandardOpenOption.CREATE_NEW,
                    StandardOpenOption.WRITE
                )
            )
        );

    final Supplier<ReadableFrameChannel> readableChannelSupplier = Suppliers.memoize(
        () -> {
          try {
            final FrameFile frameFile = FrameFile.open(file, FrameFile.Flag.DELETE_ON_CLOSE);
            return new ReadableFileFrameChannel(frameFile);
          }
          catch (IOException e) {
            throw new RuntimeException(e);
          }
        }
    )::get;

    return OutputChannel.pair(
        writableChannel,
        ArenaMemoryAllocator.createOnHeap(frameSize),
        readableChannelSupplier,
        partitionNumber
    );
  }

  @Override
  public OutputChannel openNilChannel(final int partitionNumber)
  {
    return OutputChannel.nil(partitionNumber);
  }
}
