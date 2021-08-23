/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.loading;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Holder for all the custom metrics for the virtual segment extension
 */
public class VirtualSegmentStats
{
  private long downloadTimeInMS = 0L;
  private long numBytesDownloaded = 0L;
  private AtomicLong numSegmentsEvicted = new AtomicLong();
  private AtomicLong numSegmentsRemoved = new AtomicLong();
  private AtomicLong numSegmentsQueued = new AtomicLong();
  private AtomicLong numSegmentsDownloaded = new AtomicLong();
  private long numSegmentsWaitingToDownLoad = 0L;
  private long segmentsDownLoadWatingTimeInMS = 0L;


  /**
   * Adding synchronized as we need to update two variables. The synchronization overhead is low since methods are called infrequently and have simple operations.
   */
  protected synchronized void recordDownloadTime(long curSegmentDownloadTimeInMS, long segmentSizeInBytes)
  {
    downloadTimeInMS += curSegmentDownloadTimeInMS;
    numBytesDownloaded += segmentSizeInBytes;
  }

  public synchronized long getDownloadThroughputBytesPerSecond()
  {
    return downloadTimeInMS == 0
           ? 0
           : numBytesDownloaded * 1000 / (downloadTimeInMS);
  }


  public synchronized void recordDownloadWaitingTime(long currentDownloadWaitingTimeInMS)
  {
    segmentsDownLoadWatingTimeInMS += currentDownloadWaitingTimeInMS;
    numSegmentsWaitingToDownLoad++;
  }


  public synchronized long getAvgDownloadWaitingTimeInMS()
  {
    return numSegmentsWaitingToDownLoad == 0 ? 0 : segmentsDownLoadWatingTimeInMS
                                                   / numSegmentsWaitingToDownLoad;
  }

  public long incrementEvicted()
  {
    return this.numSegmentsEvicted.incrementAndGet();
  }

  public long getNumSegmentsEvicted()
  {
    return this.numSegmentsEvicted.get();
  }

  public long incrementNumSegmentRemoved()
  {
    return this.numSegmentsRemoved.incrementAndGet();
  }

  public long getNumSegmentsRemoved()
  {
    return this.numSegmentsRemoved.get();
  }

  public long incrementQueued()
  {
    return this.numSegmentsQueued.incrementAndGet();
  }

  public long getNumSegmentsQueued()
  {
    return this.numSegmentsQueued.get();
  }

  public long incrementDownloaded()
  {
    return this.numSegmentsDownloaded.incrementAndGet();
  }

  public long getNumSegmentsDownloaded()
  {
    return this.numSegmentsDownloaded.get();
  }

  public void resetMetrics()
  {
    // taking lock on variables which need to be set in one go.
    synchronized (this) {
      downloadTimeInMS = 0;
      numBytesDownloaded = 0;
      numSegmentsWaitingToDownLoad = 0;
      segmentsDownLoadWatingTimeInMS = 0;
    }
    numSegmentsEvicted.set(0);
    numSegmentsRemoved.set(0);
    numSegmentsQueued.set(0);
    numSegmentsDownloaded.set(0);
  }
}
