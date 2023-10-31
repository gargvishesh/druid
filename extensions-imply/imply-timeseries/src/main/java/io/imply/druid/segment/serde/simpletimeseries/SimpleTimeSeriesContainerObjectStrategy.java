/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.segment.serde.simpletimeseries;

import io.imply.druid.timeseries.SimpleTimeSeriesContainer;
import org.apache.druid.error.DruidException;
import org.apache.druid.segment.data.ObjectStrategy;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.nio.ByteBuffer;

public class SimpleTimeSeriesContainerObjectStrategy implements ObjectStrategy<SimpleTimeSeriesContainer>
{

  @Override
  public Class<? extends SimpleTimeSeriesContainer> getClazz()
  {
    return SimpleTimeSeriesContainer.class;
  }

  @Override
  public SimpleTimeSeriesContainer fromByteBuffer(ByteBuffer buffer, int numBytes)
  {
    throw DruidException.defensive("Unsupported deprecated usage");
  }

  @Override
  @Nonnull
  public byte[] toBytes(@Nullable SimpleTimeSeriesContainer val)
  {
    throw DruidException.defensive("Unsupported deprecated usage");
  }

  @Override
  public int compare(SimpleTimeSeriesContainer o1, SimpleTimeSeriesContainer o2)
  {
    throw DruidException.forPersona(DruidException.Persona.USER)
                        .ofCategory(DruidException.Category.INVALID_INPUT)
                        .build("The query requires comparing timeseries objects which is unsupported. "
                               + "Consider wrapping the timeseries objects in an expression to make them comparable.");
  }
}
