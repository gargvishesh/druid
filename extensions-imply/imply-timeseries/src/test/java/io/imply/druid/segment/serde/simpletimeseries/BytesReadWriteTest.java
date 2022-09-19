/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.segment.serde.simpletimeseries;

public interface BytesReadWriteTest
{
  void testSingleWriteBytes() throws Exception;

  void testSingleMultiBlockWriteBytes() throws Exception;

  void testSingleMultiBlockWriteBytesWithPrelude() throws Exception;

  void testEmptyByteArray() throws Exception;

  void testNull() throws Exception;

  void testSingleLong() throws Exception;

  void testVariableSizedCompressablePayloads() throws Exception;

  void testOutliersInNormalDataUncompressablePayloads() throws Exception;

  void testOutliersInNormalDataCompressablePayloads() throws Exception;

  void testSingleUncompressableBlock() throws Exception;

  void testSingleWriteByteBufferZSTD() throws Exception;

  void testRandomBlockAccess() throws Exception;
}
