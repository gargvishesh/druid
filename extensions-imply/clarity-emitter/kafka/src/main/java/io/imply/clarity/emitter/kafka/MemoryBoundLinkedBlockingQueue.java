/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.clarity.emitter.kafka;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Similar to LinkedBlockingQueue but can be bounded by the total byte size of the items present in the queue
 * rather than number of items.
 */
public class MemoryBoundLinkedBlockingQueue<T>
{
  private final long memoryBound;
  private final AtomicLong currentMemory;
  private final LinkedBlockingQueue<ObjectContainer<T>> queue;

  public MemoryBoundLinkedBlockingQueue(long memoryBound)
  {
    this.memoryBound = memoryBound;
    this.currentMemory = new AtomicLong(0L);
    this.queue = new LinkedBlockingQueue<>();
  }

  // returns true/false depending on whether item was added or not
  public boolean offer(ObjectContainer<T> item)
  {
    final long itemLength = item.getSize();

    if (currentMemory.addAndGet(itemLength) <= memoryBound) {
      if (queue.offer(item)) {
        return true;
      }
    }
    currentMemory.addAndGet(-itemLength);
    return false;
  }

  // blocks until at least one item is available to take
  public ObjectContainer<T> take() throws InterruptedException
  {
    final ObjectContainer<T> ret = queue.take();
    currentMemory.addAndGet(-ret.getSize());
    return ret;
  }

  public int size()
  {
    return queue.size();
  }

  public static class ObjectContainer<T>
  {
    private T data;
    private long size;

    ObjectContainer(T data, long size)
    {
      this.data = data;
      this.size = size;
    }

    public T getData()
    {
      return data;
    }

    public long getSize()
    {
      return size;
    }
  }
}
