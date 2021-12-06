/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.talaria.frame.file;

import io.imply.druid.talaria.frame.FrameTestUtil;
import io.imply.druid.talaria.frame.TestFrameSequenceBuilder;
import org.apache.druid.segment.StorageAdapter;
import org.apache.druid.segment.TestIndex;
import org.apache.druid.segment.incremental.IncrementalIndexStorageAdapter;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

@RunWith(Parameterized.class)
public class FrameFileHttpResponseHandlerTest extends InitializedNullHandlingTest
{
  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  private final int maxRowsPerFrame;

  private StorageAdapter adapter;
  private File file;

  public FrameFileHttpResponseHandlerTest(final int maxRowsPerFrame)
  {
    this.maxRowsPerFrame = maxRowsPerFrame;
  }

  @Parameterized.Parameters(name = "maxRowsPerFrame = {0}, chunkSize = {1}")
  public static Iterable<Object[]> constructorFeeder()
  {
    final List<Object[]> constructors = new ArrayList<>();

    for (int maxRowsPerFrame : new int[]{1, 50, Integer.MAX_VALUE}) {
      constructors.add(new Object[]{maxRowsPerFrame});
    }

    return constructors;
  }

  @Before
  public void setUp() throws IOException
  {
    adapter = new IncrementalIndexStorageAdapter(TestIndex.getIncrementalTestIndex());

    file = FrameTestUtil.writeFrameFile(
        TestFrameSequenceBuilder.fromAdapter(adapter).maxRowsPerFrame(maxRowsPerFrame).frames(),
        temporaryFolder.newFile()
    );
  }

  @Test
  public void testNonChunkedResponse()
  {
    // TODO(gianm): Write handler test
  }

  @Test
  public void testChunkedResponse()
  {
    // TODO(gianm): Write handler test
  }

  @Test
  public void testErrorBeforeResponse()
  {
    // TODO(gianm): Write handler test
  }

  @Test
  public void testErrorMidResponse()
  {
    // TODO(gianm): Write handler test
  }
}
