/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.talaria.input;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonTypeName;
import org.apache.druid.data.input.InputFormat;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.InputRowListPlusRawValues;
import org.apache.druid.data.input.InputRowSchema;
import org.apache.druid.data.input.InputSource;
import org.apache.druid.data.input.InputSourceReader;
import org.apache.druid.java.util.common.CloseableIterators;
import org.apache.druid.java.util.common.parsers.CloseableIterator;

import javax.annotation.Nullable;
import java.io.File;
import java.util.Collections;

/**
 * An {@link InputSource} that returns nothing (no rows).
 */
@JsonTypeName("nil")
public class NilInputSource implements InputSource
{
  private static final NilInputSource INSTANCE = new NilInputSource();

  private NilInputSource()
  {
    // Singleton.
  }

  @JsonCreator
  public static NilInputSource instance()
  {
    return INSTANCE;
  }

  @Override
  public boolean isSplittable()
  {
    return false;
  }

  @Override
  public boolean needsFormat()
  {
    return false;
  }

  @Override
  public InputSourceReader reader(
      final InputRowSchema inputRowSchema,
      @Nullable final InputFormat inputFormat,
      final File temporaryDirectory
  )
  {
    return new InputSourceReader()
    {
      @Override
      public CloseableIterator<InputRow> read()
      {
        return CloseableIterators.wrap(Collections.emptyIterator(), () -> {});
      }

      @Override
      public CloseableIterator<InputRowListPlusRawValues> sample()
      {
        return CloseableIterators.wrap(Collections.emptyIterator(), () -> {});
      }
    };
  }
}
