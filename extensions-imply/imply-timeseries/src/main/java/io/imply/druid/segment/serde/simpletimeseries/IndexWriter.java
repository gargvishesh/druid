/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.segment.serde.simpletimeseries;

import com.google.common.base.Preconditions;
import org.apache.druid.segment.writeout.WriteOutBytes;

import java.io.IOException;
import java.nio.channels.WritableByteChannel;

public class IndexWriter
{
  private final WriteOutBytes outBytes;
  private final NumberSerializer positionSerializer;
  private final NumberSerializer indexSizeSerializer;

  private boolean open = true;
  private long position = 0;

  public IndexWriter(
      WriteOutBytes outBytes,
      NumberSerializer positionSerializer,
      NumberSerializer indexSizeSerializer
  )
  {
    this.outBytes = outBytes;
    this.positionSerializer = positionSerializer;
    this.indexSizeSerializer = indexSizeSerializer;
  }

  public IndexWriter(WriteOutBytes outBytes, NumberSerializer positionSerializer)
  {
    this(outBytes, positionSerializer, new IntSerializer());
  }

  public void persistAndIncrement(int increment) throws IOException
  {
    Preconditions.checkArgument(increment >= 0, "increment must be non-negative");
    Preconditions.checkState(open, "peristAndIncrement() must be called when open");
    outBytes.write(positionSerializer.serialize(position));
    position += increment;
  }

  void close() throws IOException
  {
    // when done, write an n+1'th entry for the next unused block; let us use invariant
    // of length of block i = i+1 - i for all i < n
    outBytes.write(positionSerializer.serialize(position));
    open = false;
  }

  public void transferTo(WritableByteChannel channel) throws IOException
  {
    channel.write(indexSizeSerializer.serialize(outBytes.size()));
    outBytes.writeTo(channel);
  }

  public long getSerializedSize()
  {
    return indexSizeSerializer.getSerializedSize() + outBytes.size();
  }
}
