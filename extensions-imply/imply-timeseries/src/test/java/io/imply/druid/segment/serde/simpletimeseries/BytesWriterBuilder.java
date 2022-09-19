/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.segment.serde.simpletimeseries;

import org.apache.druid.segment.data.CompressionStrategy;

import java.io.IOException;

// TODO: can we move to test code and have each test class implement an adapater ?
/**
 * this interface is used so that both RowWriter[.Builder] and BlockCompressedPayloadScribe[.Builder] may use the
 * same test code. production code should not use this and use the classes directly
 */

public interface BytesWriterBuilder
{
  BytesWriter build() throws IOException;

  BytesWriterBuilder setCompressionStrategy(CompressionStrategy compressionStrategy);
}
