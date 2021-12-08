/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.talaria.frame.file;

import com.google.common.base.Preconditions;
import com.google.common.io.Files;
import io.imply.druid.talaria.frame.read.Frame;
import org.apache.datasketches.memory.MapHandle;
import org.apache.datasketches.memory.Memory;
import org.apache.druid.java.util.common.ByteBufferUtils;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.IOE;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.segment.ReferenceCountingCloseableObject;

import java.io.Closeable;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteOrder;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Arrays;
import java.util.EnumSet;

/**
 * TODO(gianm): document file format
 */
public class FrameFile implements Closeable
{
  private static final Logger log = new Logger(FrameFile.class);

  public enum Flag
  {
    /**
     * Map using ByteBUsed only for testing.
     */
    BB_MEMORY_MAP,
    DS_MEMORY_MAP,
    DELETE_ON_CLOSE
  }

  private final File file;
  private final Memory memory;
  private final int numFrames;
  private final int numPartitions;
  private final ReferenceCountingCloseableObject<Closeable> referenceCounter;
  private final Closeable referenceReleaser;

  private FrameFile(
      final File file,
      final Memory memory,
      final int numFrames,
      final int numPartitions,
      final ReferenceCountingCloseableObject<Closeable> referenceCounter,
      final Closeable referenceReleaser
  )
  {
    this.file = file;
    this.memory = memory;
    this.numFrames = numFrames;
    this.numPartitions = numPartitions;
    this.referenceCounter = referenceCounter;
    this.referenceReleaser = referenceReleaser;
  }

  public static FrameFile open(final File file, final Flag... flags) throws IOException
  {
    final EnumSet<Flag> flagSet = flags.length == 0 ? EnumSet.noneOf(Flag.class) : EnumSet.copyOf(Arrays.asList(flags));

    if (!file.exists()) {
      throw new FileNotFoundException(StringUtils.format("File [%s] not found", file));
    }

    final Pair<Memory, Closeable> map = mapFile(file, flagSet);
    final Memory memory = map.lhs;
    final Closeable closer = Preconditions.checkNotNull(map.rhs, "closer");

    if (memory.getCapacity() < Integer.BYTES * 2) {
      throw new IOE("File [%s] is too short for a header", file);
    }

    // TODO(gianm): Validation of file format, magic, length, etc.
    // TODO(gianm): consider whether 2B frames is enough for a single file
    final int numFrames = memory.getInt(memory.getCapacity() - Integer.BYTES * 2);
    final int numPartitions = memory.getInt(memory.getCapacity() - Integer.BYTES);

    final Closer fileCloser = Closer.create();
    fileCloser.register(closer);

    if (flagSet.contains(Flag.DELETE_ON_CLOSE)) {
      fileCloser.register(() -> {
        if (!file.delete()) {
          log.warn("Could not delete frame file [%s]", file);
        }
      });
    }

    final ReferenceCountingCloseableObject<Closeable> referenceCounter =
        new ReferenceCountingCloseableObject<Closeable>(fileCloser) {};

    return new FrameFile(file, memory, numFrames, numPartitions, referenceCounter, referenceCounter);
  }

  private static Pair<Memory, Closeable> mapFile(final File file, final EnumSet<Flag> flagSet) throws IOException
  {
    if (flagSet.contains(Flag.DS_MEMORY_MAP) && flagSet.contains(Flag.BB_MEMORY_MAP)) {
      throw new ISE("Cannot open with both [%s] and [%s]", Flag.DS_MEMORY_MAP, Flag.BB_MEMORY_MAP);
    } else if (flagSet.contains(Flag.DS_MEMORY_MAP)) {
      return mapFileDS(file);
    } else if (flagSet.contains(Flag.BB_MEMORY_MAP)) {
      return mapFileBB(file);
    } else if (file.length() <= Integer.MAX_VALUE) {
      // Prefer using ByteBuffer for small files, because "open" can use it to avoid a copy when decompressing.
      return mapFileBB(file);
    } else {
      return mapFileDS(file);
    }
  }

  /**
   * Maps a file using a MappedByteBuffer. This is preferred for small files, since it enables zero-copy decompression
   * in {@link #open}.
   */
  private static Pair<Memory, Closeable> mapFileBB(final File file) throws IOException
  {
    final MappedByteBuffer byteBuffer = Files.map(file, FileChannel.MapMode.READ_ONLY);
    byteBuffer.order(ByteOrder.LITTLE_ENDIAN);
    return Pair.of(Memory.wrap(byteBuffer, ByteOrder.LITTLE_ENDIAN), () -> ByteBufferUtils.unmap(byteBuffer));
  }

  /**
   * Maps a file using the functionality in datasketches-memory.
   */
  private static Pair<Memory, Closeable> mapFileDS(final File file)
  {
    final MapHandle mapHandle = Memory.map(file, 0, file.length(), ByteOrder.LITTLE_ENDIAN);
    return Pair.of(
        mapHandle.get(),
        () -> {
          try {
            mapHandle.close();
          }
          catch (Exception e) {
            throw new RuntimeException(e);
          }
        }
    );
  }

  public int numFrames()
  {
    return numFrames;
  }

  public int numPartitions()
  {
    return numPartitions;
  }

  public int getPartitionStartFrame(final int partition)
  {
    checkOpen();

    if (partition < 0) {
      throw new IAE("Partition [%,d] out of bounds", partition);
    } else if (partition >= numPartitions) {
      // Frame might not have every partition, if some are empty.
      return numFrames;
    } else {
      return memory.getInt(
          memory.getCapacity()
          - FrameFileWriter.FOOTER_LENGTH
          - (long) numFrames * Long.BYTES
          - (long) (numPartitions - partition) * Integer.BYTES
      );
    }
  }

  /**
   * TODO(gianm): this causes an allocation and decompression and copy. provide a way to pass in memory to avoid allocation
   */
  public Frame frame(final int frameNumber)
  {
    checkOpen();

    if (frameNumber < 0 || frameNumber >= numFrames) {
      throw new IAE("Frame [%,d] out of bounds", frameNumber);
    }

    final long regionEndPointer =
        memory.getCapacity() - FrameFileWriter.FOOTER_LENGTH - (long) (numFrames - frameNumber) * Long.BYTES;
    final long regionEnd = memory.getLong(regionEndPointer);
    final long regionStart;

    if (frameNumber == 0) {
      regionStart = FrameFileWriter.MAGIC.length + Byte.BYTES;
    } else {
      regionStart = memory.getLong(regionEndPointer - Long.BYTES) + Byte.BYTES;
    }

    return Frame.decompress(memory, regionStart, regionEnd - regionStart);
  }

  public FrameFile newReference()
  {
    final Closeable releaser = referenceCounter.incrementReferenceAndDecrementOnceCloseable()
                                               .orElseThrow(() -> new ISE("Frame file is closed"));

    return new FrameFile(file, memory, numFrames, numPartitions, referenceCounter, releaser);
  }

  /**
   * Returns the file that this instance is backed by.
   */
  public File file()
  {
    return file;
  }

  @Override
  public void close() throws IOException
  {
    referenceReleaser.close();
  }

  /**
   * Checks if the frame file is open. If so, does nothing. If not, throws an exception.
   *
   * Racey, since this object can be used by multiple threads, but this is only meant as a last-ditch sanity check, not
   * a bulletproof precondition check.
   */
  private void checkOpen()
  {
    if (referenceCounter.isClosed()) {
      throw new ISE("Frame file is closed");
    }
  }
}
