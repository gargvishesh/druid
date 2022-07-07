/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.segment.serde.simpletimeseries;

import io.imply.druid.timeseries.utils.ImplyLongArrayList;

import javax.annotation.Nonnull;
import java.nio.ByteBuffer;

public interface TimestampsEncoderDecoder
{
  StorableList encode(@Nonnull ImplyLongArrayList timestamps);

  ImplyLongArrayList decode(@Nonnull ByteBuffer byteBuffer, boolean isRle);
}
