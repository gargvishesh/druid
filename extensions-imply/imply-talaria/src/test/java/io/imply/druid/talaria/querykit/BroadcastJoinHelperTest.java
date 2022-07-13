/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.talaria.querykit;

import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.ListenableFuture;
import io.imply.druid.talaria.frame.ArenaMemoryAllocator;
import io.imply.druid.talaria.frame.Frame;
import io.imply.druid.talaria.frame.FrameType;
import io.imply.druid.talaria.frame.channel.ReadableFileFrameChannel;
import io.imply.druid.talaria.frame.channel.ReadableFrameChannel;
import io.imply.druid.talaria.frame.channel.Try;
import io.imply.druid.talaria.frame.file.FrameFile;
import io.imply.druid.talaria.frame.read.FrameReader;
import io.imply.druid.talaria.frame.testutil.FrameSequenceBuilder;
import io.imply.druid.talaria.frame.testutil.FrameTestUtil;
import io.imply.druid.talaria.indexing.error.BroadcastTablesTooLargeFault;
import io.imply.druid.talaria.indexing.error.TalariaException;
import it.unimi.dsi.fastutil.ints.Int2IntMap;
import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.ints.IntSet;
import org.apache.druid.java.util.common.guava.Sequences;
import org.apache.druid.math.expr.ExprMacroTable;
import org.apache.druid.query.DataSource;
import org.apache.druid.query.InlineDataSource;
import org.apache.druid.query.JoinDataSource;
import org.apache.druid.segment.QueryableIndexStorageAdapter;
import org.apache.druid.segment.StorageAdapter;
import org.apache.druid.segment.TestIndex;
import org.apache.druid.segment.join.JoinConditionAnalysis;
import org.apache.druid.segment.join.JoinType;
import org.apache.druid.segment.join.JoinableFactory;
import org.apache.druid.segment.join.JoinableFactoryWrapper;
import org.apache.druid.server.QueryStackTests;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.hamcrest.CoreMatchers;
import org.hamcrest.MatcherAssert;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class BroadcastJoinHelperTest extends InitializedNullHandlingTest
{
  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  private JoinableFactory joinableFactory;
  private StorageAdapter adapter;
  private File testDataFile1;
  private File testDataFile2;
  private FrameReader frameReader1;
  private FrameReader frameReader2;

  @Before
  public void setUp() throws IOException
  {
    final ArenaMemoryAllocator allocator = ArenaMemoryAllocator.createOnHeap(10_000);

    joinableFactory = QueryStackTests.makeJoinableFactoryFromDefault(null, null, null);

    adapter = new QueryableIndexStorageAdapter(TestIndex.getNoRollupMMappedTestIndex());

    // File 1: the entire test dataset.
    testDataFile1 = FrameTestUtil.writeFrameFile(
        FrameSequenceBuilder.fromAdapter(adapter)
                            .frameType(FrameType.ROW_BASED) // No particular reason to test with both frame types
                            .allocator(allocator)
                            .frames(),
        temporaryFolder.newFile()
    );

    // File 2: just two rows.
    testDataFile2 = FrameTestUtil.writeFrameFile(
        FrameSequenceBuilder.fromAdapter(adapter)
                            .frameType(FrameType.ROW_BASED) // No particular reason to test with both frame types
                            .allocator(allocator)
                            .maxRowsPerFrame(1)
                            .frames()
                            .limit(2),
        temporaryFolder.newFile()
    );

    frameReader1 = FrameReader.create(adapter.getRowSignature());
    frameReader2 = FrameReader.create(adapter.getRowSignature());
  }

  @Test
  public void testBuildTableAndInlineData() throws IOException
  {
    final Int2IntMap sideStageChannelNumberMap = new Int2IntOpenHashMap();
    sideStageChannelNumberMap.put(3, 1);
    sideStageChannelNumberMap.put(4, 2);

    final List<ReadableFrameChannel> channels = new ArrayList<>();
    channels.add(new ExplodingReadableFrameChannel());
    channels.add(new ReadableFileFrameChannel(FrameFile.open(testDataFile1)));
    channels.add(new ReadableFileFrameChannel(FrameFile.open(testDataFile2)));

    final List<FrameReader> channelReaders = new ArrayList<>();
    channelReaders.add(null);
    channelReaders.add(frameReader1);
    channelReaders.add(frameReader2);

    final BroadcastJoinHelper broadcastJoinHelper = new BroadcastJoinHelper(
        sideStageChannelNumberMap,
        channels,
        channelReaders,
        new JoinableFactoryWrapper(joinableFactory),
        25_000_000L // High enough memory limit that we won't hit it
    );

    Assert.assertEquals(ImmutableSet.of(1, 2), broadcastJoinHelper.getSideChannelNumbers());

    boolean doneReading = false;
    while (!doneReading) {
      final IntSet readableInputs = new IntOpenHashSet();
      for (int i = 1; i < channels.size(); i++) {
        readableInputs.add(i); // Frame file channels are always ready, so this is OK.
      }
      doneReading = broadcastJoinHelper.buildBroadcastTablesIncrementally(readableInputs);
    }

    Assert.assertTrue(channels.get(1).isFinished());
    Assert.assertTrue(channels.get(2).isFinished());

    Assert.assertEquals(
        new InputNumberDataSource(0),
        broadcastJoinHelper.inlineChannelData(new InputNumberDataSource(0))
    );

    Assert.assertEquals(
        new InputNumberDataSource(1),
        broadcastJoinHelper.inlineChannelData(new InputNumberDataSource(1))
    );

    Assert.assertEquals(
        new InputNumberDataSource(2),
        broadcastJoinHelper.inlineChannelData(new InputNumberDataSource(2))
    );

    final List<Object[]> rowsFromStage3 =
        ((InlineDataSource) broadcastJoinHelper.inlineChannelData(new InputNumberDataSource(3))).getRowsAsList();
    Assert.assertEquals(1209, rowsFromStage3.size());

    FrameTestUtil.assertRowsEqual(
        FrameTestUtil.readRowsFromAdapter(adapter, null, false),
        Sequences.simple(rowsFromStage3.stream().map(Arrays::asList).collect(Collectors.toList()))
    );

    final List<Object[]> rowsFromStage4 =
        ((InlineDataSource) broadcastJoinHelper.inlineChannelData(new InputNumberDataSource(4))).getRowsAsList();
    Assert.assertEquals(2, rowsFromStage4.size());

    FrameTestUtil.assertRowsEqual(
        FrameTestUtil.readRowsFromAdapter(adapter, null, false).limit(2),
        Sequences.simple(rowsFromStage4.stream().map(Arrays::asList).collect(Collectors.toList()))
    );

    final DataSource inlinedJoinDataSource = broadcastJoinHelper.inlineChannelData(
        JoinDataSource.create(
            new InputNumberDataSource(0),
            new InputNumberDataSource(4),
            "j.",
            JoinConditionAnalysis.forExpression("x == \"j.x\"", "j.", ExprMacroTable.nil()),
            JoinType.INNER,
            null
        )
    );

    MatcherAssert.assertThat(
        ((JoinDataSource) inlinedJoinDataSource).getRight(),
        CoreMatchers.instanceOf(InlineDataSource.class)
    );

    Assert.assertEquals(
        2,
        ((InlineDataSource) ((JoinDataSource) inlinedJoinDataSource).getRight()).getRowsAsList().size()
    );
  }

  @Test
  public void testBuildTableMemoryLimit() throws IOException
  {
    final Int2IntMap sideStageChannelNumberMap = new Int2IntOpenHashMap();
    sideStageChannelNumberMap.put(0, 0);

    final List<ReadableFrameChannel> channels = new ArrayList<>();
    channels.add(new ReadableFileFrameChannel(FrameFile.open(testDataFile1)));

    final List<FrameReader> channelReaders = new ArrayList<>();
    channelReaders.add(frameReader1);

    final BroadcastJoinHelper broadcastJoinHelper = new BroadcastJoinHelper(
        sideStageChannelNumberMap,
        channels,
        channelReaders,
        new JoinableFactoryWrapper(joinableFactory),
        100_000 // Low memory limit; we will hit this
    );

    Assert.assertEquals(ImmutableSet.of(0), broadcastJoinHelper.getSideChannelNumbers());

    final TalariaException e = Assert.assertThrows(
        TalariaException.class,
        () -> {
          boolean doneReading = false;
          while (!doneReading) {
            final IntSet readableInputs = new IntOpenHashSet(new int[]{0});
            doneReading = broadcastJoinHelper.buildBroadcastTablesIncrementally(readableInputs);
          }
        }
    );

    Assert.assertEquals(new BroadcastTablesTooLargeFault(100_000), e.getFault());
  }

  /**
   * Throws an error on every method call. Useful for ensuring that a channel is *not* read.
   */
  private static class ExplodingReadableFrameChannel implements ReadableFrameChannel
  {
    @Override
    public boolean isFinished()
    {
      throw new IllegalStateException();
    }

    @Override
    public boolean canRead()
    {
      throw new IllegalStateException();
    }

    @Override
    public Try<Frame> read()
    {
      throw new IllegalStateException();
    }

    @Override
    public ListenableFuture<?> readabilityFuture()
    {
      throw new IllegalStateException();
    }

    @Override
    public void doneReading()
    {
      throw new IllegalStateException();
    }
  }
}
