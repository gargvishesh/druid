/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.talaria.frame.channel;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.primitives.Ints;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import com.google.errorprone.annotations.concurrent.GuardedBy;
import io.imply.druid.talaria.frame.Frame;
import io.imply.druid.talaria.frame.file.FrameFileWriter;
import org.apache.datasketches.memory.Memory;
import org.apache.datasketches.memory.WritableMemory;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.logger.Logger;

import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Optional;

/**
 * TODO(gianm): Javadocs
 * TODO(gianm): Note that the limit is 'loose' (single frame may exceed it)
 * TODO(gianm): Make sure we can cleanly detect truncation; & include tests
 */
public class ReadableByteChunksFrameChannel implements ReadableFrameChannel
{
  private static final Logger log = new Logger(ReadableByteChunksFrameChannel.class);

  private static final int UNKNOWN_LENGTH = -1;
  private static final int FRAME_MARKER_AND_LENGTH_BYTES = Byte.BYTES + Long.BYTES * 2;

  private enum StreamPart
  {
    MAGIC,
    FRAMES,
    FOOTER
  }

  private final Object lock = new Object();
  private final String id;
  private final long bytesLimit;

  @GuardedBy("lock")
  private final List<Try<byte[]>> chunks = new ArrayList<>();

  @GuardedBy("lock")
  private SettableFuture<?> addChunkBackpressureFuture = null;

  @GuardedBy("lock")
  private SettableFuture<?> readyForReadingFuture = null;

  @GuardedBy("lock")
  private boolean noMoreWrites = false;

  @GuardedBy("lock")
  private int positionInFirstChunk = 0;

  @GuardedBy("lock")
  private long bytesBuffered = 0;

  @GuardedBy("lock")
  private long bytesAdded = 0;

  @GuardedBy("lock")
  private long nextCompressedFrameLength = UNKNOWN_LENGTH;

  @GuardedBy("lock")
  private StreamPart streamPart = StreamPart.MAGIC;

  private ReadableByteChunksFrameChannel(String id, long bytesLimit)
  {
    this.id = Preconditions.checkNotNull(id, "id");
    this.bytesLimit = bytesLimit;
  }

  public static ReadableByteChunksFrameChannel create(final String id)
  {
    return new ReadableByteChunksFrameChannel(id, 1);
  }

  /**
   * Adds a chunk of bytes. If this chunk forms a full frame, it will immediately become available for reading.
   * Otherwise, the bytes will be buffered until a full frame is encountered.
   *
   * Returns an Optional that is absent if the amount of queued bytes is below this channel's limit accept, or present
   * if the amount of queued bytes is at or above this channel's limit. If the Optional is present, you are politely
   * requested to wait for the future to resolve before adding additional chunks. (This is not enforced; addChunk will
   * continue to accept new chunks even if the channel is over its limit.)
   *
   * When done adding chunks call {@code doneWriting}.
   */
  public Optional<ListenableFuture<?>> addChunk(final byte[] chunk)
  {
    synchronized (lock) {
      if (noMoreWrites) {
        throw new ISE("Channel is no longer accepting writes");
      }

      try {
        if (chunk.length > 0) {
          bytesAdded += chunk.length;

          if (streamPart != StreamPart.FOOTER) {
            // TODO(gianm): Validate footer instead of throwing it away
            chunks.add(Try.value(chunk));
            bytesBuffered += chunk.length;
          }

          updateStreamState();

          if (readyForReadingFuture != null && canReadFrame()) {
            readyForReadingFuture.set(null);
            readyForReadingFuture = null;
          }
        }

        if (addChunkBackpressureFuture == null && bytesBuffered >= bytesLimit && canReadFrame()) {
          addChunkBackpressureFuture = SettableFuture.create();
        }

        return Optional.ofNullable(addChunkBackpressureFuture);
      }
      catch (Throwable e) {
        // The channel is in an inconsistent state if any of this logic throws an error. Shut it down.
        setError(e);
        return Optional.empty();
      }
    }
  }

  /**
   * Clears the channel and replaces it with the given error. After calling this method, no additional chunks will be accepted.
   */
  public void setError(final Throwable t)
  {
    synchronized (lock) {
      if (noMoreWrites) {
        log.noStackTrace().warn(t, "Channel is no longer accepting writes, cannot propagate exception");
      } else {
        chunks.clear();
        chunks.add(Try.error(t));
        nextCompressedFrameLength = UNKNOWN_LENGTH;
        doneWriting();
      }
    }
  }

  /**
   * Call method when caller is done adding chunks.
   */
  public void doneWriting()
  {
    synchronized (lock) {
      noMoreWrites = true;

      if (readyForReadingFuture != null) {
        readyForReadingFuture.set(null);
        readyForReadingFuture = null;
      }
    }
  }

  @Override
  public boolean isFinished()
  {
    synchronized (lock) {
      return chunks.isEmpty() && noMoreWrites;
    }
  }

  @Override
  public boolean canRead()
  {
    synchronized (lock) {
      // The noMoreWrites check is here so read() can throw an error if the last few chunks are an incomplete frame.
      return canReadError() || canReadFrame() || (streamPart != StreamPart.FOOTER && noMoreWrites);
    }
  }

  @Override
  public Try<Frame> read()
  {
    synchronized (lock) {
      if (canReadError()) {
        // This map will be a no-op since we're guaranteed that the next chunk is an error.
        return chunks.remove(0).map(bytes -> null);
      } else if (canReadFrame()) {
        return Try.value(nextFrame());
      } else if (noMoreWrites) {
        // The last few chunks are an incomplete or missing frame.
        chunks.clear();
        nextCompressedFrameLength = UNKNOWN_LENGTH;

        return Try.error(
            new ISE(
                "Incomplete or missing frame at end of stream (id = %s, position = %d)",
                id,
                bytesAdded - bytesBuffered
            )
        );
      } else {
        assert !canRead();

        // This method should not have been called at this time.
        throw new NoSuchElementException();
      }
    }
  }

  @Override
  public ListenableFuture<?> readabilityFuture()
  {
    synchronized (lock) {
      if (canRead() || isFinished()) {
        return Futures.immediateFuture(null);
      } else if (readyForReadingFuture != null) {
        return readyForReadingFuture;
      } else {
        return (readyForReadingFuture = SettableFuture.create());
      }
    }
  }

  @Override
  public void doneReading()
  {
    synchronized (lock) {
      // TODO(gianm): Setting "noMoreWrites" causes the upstream entity to realize this channel has closed the
      //    next time it calls "addChunk". It'd be better to propagate this information earlier, so that entity
      //    can shut itself down more promptly in case of query cancelation or downstream failure.
      noMoreWrites = true;
      chunks.clear();
      nextCompressedFrameLength = UNKNOWN_LENGTH;
    }
  }

  public String getId()
  {
    return id;
  }

  public long getBytesAdded()
  {
    synchronized (lock) {
      return bytesAdded;
    }
  }

  public boolean isErrorOrFinished()
  {
    synchronized (lock) {
      return isFinished() || canReadError();
    }
  }

  @VisibleForTesting
  long getBytesBuffered()
  {
    synchronized (lock) {
      return bytesBuffered;
    }
  }

  private Frame nextFrame()
  {
    final Memory frameMemory;

    synchronized (lock) {
      if (!canReadFrame()) {
        throw new ISE("Frame of size [%,d] not yet ready to read", nextCompressedFrameLength);
      }

      if (nextCompressedFrameLength > Integer.MAX_VALUE - FRAME_MARKER_AND_LENGTH_BYTES) {
        throw new ISE("Cannot read frame of size [%,d] bytes", nextCompressedFrameLength);
      }

      final int numBytes = Ints.checkedCast(FRAME_MARKER_AND_LENGTH_BYTES + nextCompressedFrameLength);
      frameMemory = copyFromQueuedChunks(numBytes).region(
          Byte.BYTES, /* Skip frame marker, but include the frame length information */
          FRAME_MARKER_AND_LENGTH_BYTES - Byte.BYTES + nextCompressedFrameLength
      );
      deleteFromQueuedChunks(numBytes);
      updateStreamState();
    }

    final Frame frame = Frame.decompress(frameMemory, 0, frameMemory.getCapacity());
    log.debug("Read frame with [%,d] rows and [%,d] bytes.", frame.numRows(), frame.numBytes());
    return frame;
  }

  @GuardedBy("lock")
  private void updateStreamState()
  {
    if (streamPart == StreamPart.MAGIC) {
      if (bytesBuffered >= FrameFileWriter.MAGIC.length) {
        final Memory memory = copyFromQueuedChunks(FrameFileWriter.MAGIC.length);

        if (memory.equalTo(0, Memory.wrap(FrameFileWriter.MAGIC), 0, FrameFileWriter.MAGIC.length)) {
          streamPart = StreamPart.FRAMES;
          deleteFromQueuedChunks(FrameFileWriter.MAGIC.length);
        } else {
          throw new ISE("Invalid stream header (id = %s, position = %d)", id, bytesAdded - bytesBuffered);
        }
      }
    }

    if (streamPart == StreamPart.FRAMES) {
      if (bytesBuffered >= Byte.BYTES) {
        final Memory memory = copyFromQueuedChunks(1);

        if (memory.getByte(0) == FrameFileWriter.MARKER_FRAME) {
          // Read nextFrameLength if needed; otherwise do nothing.
          if (nextCompressedFrameLength == UNKNOWN_LENGTH
              && bytesBuffered >= FRAME_MARKER_AND_LENGTH_BYTES) {
            // TODO(gianm): Prevent super-jumbo nextFrameLength; this stream may not come from fully-trusted source and
            //  we must avoid resource exhaustion
            nextCompressedFrameLength = copyFromQueuedChunks(FRAME_MARKER_AND_LENGTH_BYTES).getLong(Byte.BYTES);
          }
        } else if (memory.getByte(0) == FrameFileWriter.MARKER_NO_MORE_FRAMES) {
          streamPart = StreamPart.FOOTER;
          nextCompressedFrameLength = UNKNOWN_LENGTH;
        } else {
          throw new ISE("Invalid midstream marker (id = %s, position = %d)", id, bytesAdded - bytesBuffered);
        }
      }
    }

    if (streamPart == StreamPart.FOOTER) {
      // TODO(gianm): Validate footer instead of throwing it away
      if (bytesBuffered > 0) {
        deleteFromQueuedChunks(bytesBuffered);
      }

      assert bytesBuffered == 0 && chunks.isEmpty() && nextCompressedFrameLength == UNKNOWN_LENGTH;
    }

    if (addChunkBackpressureFuture != null && (bytesBuffered < bytesLimit || !canReadFrame())) {
      // Release backpressure.
      addChunkBackpressureFuture.set(null);
      addChunkBackpressureFuture = null;
    }
  }

  @GuardedBy("lock")
  private boolean canReadError()
  {
    return chunks.size() > 0 && chunks.get(0).isError();
  }

  @GuardedBy("lock")
  private boolean canReadFrame()
  {
    return nextCompressedFrameLength != UNKNOWN_LENGTH
           && bytesBuffered >= FRAME_MARKER_AND_LENGTH_BYTES + nextCompressedFrameLength;
  }

  @GuardedBy("lock")
  private Memory copyFromQueuedChunks(final int numBytes)
  {
    if (bytesBuffered < numBytes) {
      throw new IAE("Cannot copy [%,d] bytes, only have [%,d] buffered", numBytes, bytesBuffered);
    }

    // TODO(gianm): consistent order
    final WritableMemory buf = WritableMemory.allocate(numBytes, ByteOrder.nativeOrder());

    int bufPos = 0;
    for (int chunkNumber = 0; chunkNumber < chunks.size(); chunkNumber++) {
      final byte[] chunk = chunks.get(chunkNumber).getOrThrow();
      final int chunkPosition = chunkNumber == 0 ? positionInFirstChunk : 0;
      final int len = Math.min(chunk.length - chunkPosition, numBytes - bufPos);

      buf.putByteArray(bufPos, chunk, chunkPosition, len);
      bufPos += len;

      if (bufPos == numBytes) {
        break;
      }
    }

    return buf;
  }

  @GuardedBy("lock")
  private void deleteFromQueuedChunks(final long numBytes)
  {
    if (bytesBuffered < numBytes) {
      throw new IAE("Cannot delete [%,d] bytes, only have [%,d] buffered", numBytes, bytesBuffered);
    }

    long toDelete = numBytes;

    while (toDelete > 0) {
      final byte[] chunk = chunks.get(0).getOrThrow();
      final int bytesRemainingInChunk = chunk.length - positionInFirstChunk;

      if (toDelete >= bytesRemainingInChunk) {
        toDelete -= bytesRemainingInChunk;
        positionInFirstChunk = 0;
        chunks.remove(0);
      } else {
        positionInFirstChunk += toDelete;
        toDelete = 0;
      }
    }

    bytesBuffered -= numBytes;

    // Clear nextFrameLength; it won't be accurate anymore after deleting bytes.
    nextCompressedFrameLength = UNKNOWN_LENGTH;
  }
}
