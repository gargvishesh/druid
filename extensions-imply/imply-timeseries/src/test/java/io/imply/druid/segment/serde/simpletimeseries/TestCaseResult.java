/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.segment.serde.simpletimeseries;

public class TestCaseResult
{
  private TestCaseResult(byte[] bytes, int size)
  {
    this.bytes = bytes;
    this.size = size;
  }

  public static TestCaseResult of(int sizeBytes)
  {
    return new TestCaseResult(null, sizeBytes);
  }

  public static TestCaseResult of(byte[] bytes)
  {
    return new TestCaseResult(bytes, bytes.length);
  }

  public static TestCaseResult of(byte[] bytes, int sizeBytes)
  {
    return new TestCaseResult(bytes, sizeBytes);
  }

  public byte[] bytes;
  public int size;
}
