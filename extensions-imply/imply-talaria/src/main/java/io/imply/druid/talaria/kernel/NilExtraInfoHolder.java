/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.talaria.kernel;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonTypeName;

@JsonTypeName("nil")
public class NilExtraInfoHolder extends ExtraInfoHolder<Object>
{
  private static final NilExtraInfoHolder INSTANCE = new NilExtraInfoHolder();

  private NilExtraInfoHolder()
  {
    super(null);
  }

  @JsonCreator
  public static NilExtraInfoHolder instance()
  {
    return INSTANCE;
  }
}
