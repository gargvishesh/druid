/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.talaria.framework;

import io.imply.druid.talaria.guice.TalariaIndexingModule;

public class TalariaTestIndexingModule extends TalariaIndexingModule
{
  @Override
  protected boolean isLicensed()
  {
    return true;
  }
}
