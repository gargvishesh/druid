/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.segment.serde.simpletimeseries;

import org.apache.druid.segment.writeout.WriteOutBytes;

import java.io.IOException;
import java.nio.channels.WritableByteChannel;

public class BlockCompressedPayloadSerializer
{
  private final IntSerializer intSerializer = new IntSerializer();
  private final BlockIndexWriter blockIndexWriter;
  private final WriteOutBytes dataOutBytes;

  public BlockCompressedPayloadSerializer(BlockIndexWriter blockIndexWriter, WriteOutBytes dataOutBytes)
  {
    this.blockIndexWriter = blockIndexWriter;
    this.dataOutBytes = dataOutBytes;
  }

  public void transferTo(WritableByteChannel channel) throws IOException
  {
    blockIndexWriter.transferTo(channel);
    channel.write(intSerializer.serialize(dataOutBytes.size()));
    dataOutBytes.writeTo(channel);
  }

  public long getSerializedSize()
  {
    return +blockIndexWriter.getSerializedSize()
           + intSerializer.getSerializedSize()
           + dataOutBytes.size();
  }
}
