/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.talaria.querykit;

import com.fasterxml.jackson.core.type.TypeReference;
import io.imply.druid.talaria.frame.processor.FrameProcessor;
import io.imply.druid.talaria.frame.processor.FrameProcessorFactory;
import io.imply.druid.talaria.kernel.ExtraInfoHolder;
import io.imply.druid.talaria.kernel.NilExtraInfoHolder;
import org.apache.druid.java.util.common.ISE;

import javax.annotation.Nullable;

/**
 * Basic abstract {@link FrameProcessorFactory} that yields workers that do not require extra info and that
 * always return Longs. This base class isn't used for every worker factory, but it is used for many of them.
 */
public abstract class BaseFrameProcessorFactory
    implements FrameProcessorFactory<Object, FrameProcessor<Long>, Long, Long>
{
  @Override
  public TypeReference<Long> getAccumulatedResultTypeReference()
  {
    return new TypeReference<Long>() {};
  }

  @Override
  public Long newAccumulatedResult()
  {
    return 0L;
  }

  @Nullable
  @Override
  public Long accumulateResult(Long accumulated, Long current)
  {
    return accumulated + current;
  }

  @Override
  public Long mergeAccumulatedResult(Long accumulated, Long otherAccumulated)
  {
    return accumulated + otherAccumulated;
  }

  @Override
  public ExtraInfoHolder makeExtraInfoHolder(@Nullable Object extra)
  {
    if (extra != null) {
      throw new ISE("Expected null 'extra'");
    }

    return NilExtraInfoHolder.instance();
  }
}
