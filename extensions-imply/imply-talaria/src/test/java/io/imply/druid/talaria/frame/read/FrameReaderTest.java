/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.talaria.frame.read;

import com.google.common.collect.Iterables;
import io.imply.druid.talaria.frame.Frame;
import io.imply.druid.talaria.frame.FrameType;
import io.imply.druid.talaria.frame.HeapMemoryAllocator;
import io.imply.druid.talaria.frame.testutil.FrameSequenceBuilder;
import org.apache.druid.segment.QueryableIndexStorageAdapter;
import org.apache.druid.segment.StorageAdapter;
import org.apache.druid.segment.TestIndex;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.ArrayList;
import java.util.List;

@RunWith(Parameterized.class)
public class FrameReaderTest extends InitializedNullHandlingTest
{
  private final FrameType frameType;

  private StorageAdapter inputAdapter;
  private Frame frame;
  private FrameReader frameReader;

  public FrameReaderTest(final FrameType frameType)
  {
    this.frameType = frameType;
  }

  @Parameterized.Parameters(name = "frameType = {0}")
  public static Iterable<Object[]> constructorFeeder()
  {
    final List<Object[]> constructors = new ArrayList<>();

    for (final FrameType frameType : FrameType.values()) {
      constructors.add(new Object[]{frameType});
    }

    return constructors;
  }

  @Before
  public void setUp()
  {
    inputAdapter = new QueryableIndexStorageAdapter(TestIndex.getNoRollupMMappedTestIndex());

    final FrameSequenceBuilder frameSequenceBuilder =
        FrameSequenceBuilder.fromAdapter(inputAdapter)
                            .frameType(frameType)
                            .allocator(HeapMemoryAllocator.unlimited());

    frame = Iterables.getOnlyElement(frameSequenceBuilder.frames().toList());
    frameReader = FrameReader.create(frameSequenceBuilder.signature());
  }

  @Test
  public void testSignature()
  {
    Assert.assertEquals(inputAdapter.getRowSignature(), frameReader.signature());
  }

  @Test
  public void testColumnCapabilitiesToColumnType()
  {
    for (final String columnName : inputAdapter.getRowSignature().getColumnNames()) {
      Assert.assertEquals(
          columnName,
          inputAdapter.getRowSignature().getColumnCapabilities(columnName).toColumnType(),
          frameReader.columnCapabilities(frame, columnName).toColumnType()
      );
    }
  }
}
