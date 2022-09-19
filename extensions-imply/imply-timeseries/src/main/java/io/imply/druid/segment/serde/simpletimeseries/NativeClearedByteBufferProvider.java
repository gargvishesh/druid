/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.segment.serde.simpletimeseries;

import org.apache.druid.collections.ResourceHolder;
import org.apache.druid.segment.CompressedPools;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.function.Supplier;

/**
 * supplies clear()'d ByteBuffers wrapped in a ResourceHolder
 */
public class NativeClearedByteBufferProvider implements Supplier<ResourceHolder<ByteBuffer>>
{
  public static final NativeClearedByteBufferProvider DEFAULT = new NativeClearedByteBufferProvider();

  @Override
  public ResourceHolder<ByteBuffer> get()
  {
    ResourceHolder<ByteBuffer> holder = CompressedPools.getByteBuf(ByteOrder.nativeOrder());
    holder.get().clear();

    return holder;
  }
}
