/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.segment.serde.simpletimeseries;

import com.google.common.collect.ImmutableList;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;
import java.util.List;

public class LongValueRunLengthEncoderDecoderTest
{
  private final long[] simpleValues1 = new long[]{
      Long.MIN_VALUE,
      Long.MIN_VALUE,
      100,
      100,
      100,
      1,
      10,
      10,
      2,
      Long.MAX_VALUE,
      Long.MAX_VALUE,
      Long.MAX_VALUE,
      3,
      4,
      4,
      4,
      4
  };
  private final List<LongValueRunLengthEncoderDecoder.LongValueRun> simpleValues1Encoded =
      ImmutableList.<LongValueRunLengthEncoderDecoder.LongValueRun>builder()
                   .add(new LongValueRunLengthEncoderDecoder.LongValueRun(2, Long.MIN_VALUE))
                   .add(new LongValueRunLengthEncoderDecoder.LongValueRun(3, 100))
                   .add(new LongValueRunLengthEncoderDecoder.LongValueRun(1, 1))
                   .add(new LongValueRunLengthEncoderDecoder.LongValueRun(2, 10))
                   .add(new LongValueRunLengthEncoderDecoder.LongValueRun(1, 2))
                   .add(new LongValueRunLengthEncoderDecoder.LongValueRun(3, Long.MAX_VALUE))
                   .add(new LongValueRunLengthEncoderDecoder.LongValueRun(1, 3))
                   .add(new LongValueRunLengthEncoderDecoder.LongValueRun(4, 4))
                   .build();
  private final long[] empty = new long[0];

  @Test
  public void testEncodeSimple()
  {
    List<LongValueRunLengthEncoderDecoder.LongValueRun> encoded =
        LongValueRunLengthEncoderDecoder.encode(simpleValues1, simpleValues1.length);
    Assert.assertEquals(simpleValues1Encoded, encoded);
  }

  @Test
  public void testSingleValue()
  {
    List<LongValueRunLengthEncoderDecoder.LongValueRun> encoded =
        LongValueRunLengthEncoderDecoder.encode(new long[]{Long.MAX_VALUE}, 1);
    Assert.assertEquals(
        Collections.singletonList(new LongValueRunLengthEncoderDecoder.LongValueRun(1, Long.MAX_VALUE)), encoded
    );
  }

  @Test
  public void testEncodeEmpty()
  {
    List<LongValueRunLengthEncoderDecoder.LongValueRun> encoded =
        LongValueRunLengthEncoderDecoder.encode(empty, 0);
    Assert.assertEquals(Collections.emptyList(), encoded);
  }

  @Test
  public void testEmptyBuffer()
  {
    long[] decoded = LongValueRunLengthEncoderDecoder.decode(SimpleTimeSeriesTestUtil.EMPTY_BYTE_BUFFER);
    Assert.assertEquals(0, decoded.length);
  }

}
