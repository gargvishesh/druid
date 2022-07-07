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

public class IntegerValueRunLengthEncoderDecoderTest
{
  private final int[] simpleValues1 = new int[]{100, 100, 100, 1, 10, 10, 2, 3, 4, 4, 4, 4};
  private final List<IntegerValueRunLengthEncoderDecoder.IntegerValueRun> simpleValues1Encoded =
      ImmutableList.<IntegerValueRunLengthEncoderDecoder.IntegerValueRun>builder()
                   .add(new IntegerValueRunLengthEncoderDecoder.IntegerValueRun(3, 100))
                   .add(new IntegerValueRunLengthEncoderDecoder.IntegerValueRun(1, 1))
                   .add(new IntegerValueRunLengthEncoderDecoder.IntegerValueRun(2, 10))
                   .add(new IntegerValueRunLengthEncoderDecoder.IntegerValueRun(1, 2))
                   .add(new IntegerValueRunLengthEncoderDecoder.IntegerValueRun(1, 3))
                   .add(new IntegerValueRunLengthEncoderDecoder.IntegerValueRun(4, 4))
                   .build();
  private final int[] empty = new int[0];

  @Test
  public void testEncodeSimple()
  {
    List<IntegerValueRunLengthEncoderDecoder.IntegerValueRun> encoded =
        IntegerValueRunLengthEncoderDecoder.encode(simpleValues1, simpleValues1.length);
    Assert.assertEquals(simpleValues1Encoded, encoded);
  }

  @Test
  public void testSingleValue()
  {
    List<IntegerValueRunLengthEncoderDecoder.IntegerValueRun> encoded =
        IntegerValueRunLengthEncoderDecoder.encode(new int[]{Integer.MAX_VALUE}, 1);
    Assert.assertEquals(
        Collections.singletonList(new IntegerValueRunLengthEncoderDecoder.IntegerValueRun(1, Integer.MAX_VALUE)),
        encoded
    );
  }


  @Test
  public void testEncodeEmpty()
  {
    List<IntegerValueRunLengthEncoderDecoder.IntegerValueRun> encoded =
        IntegerValueRunLengthEncoderDecoder.encode(empty, 0);
    Assert.assertEquals(Collections.emptyList(), encoded);
  }

  @Test
  public void testEmptyBuffer()
  {
    long[] decoded = IntegerValueRunLengthEncoderDecoder.decode(SimpleTimeSeriesTestUtil.EMPTY_BYTE_BUFFER);
    Assert.assertEquals(0, decoded.length);
  }
}
