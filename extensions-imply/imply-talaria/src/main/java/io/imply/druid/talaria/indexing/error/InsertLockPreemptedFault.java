/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.talaria.indexing.error;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonTypeName;

@JsonTypeName(InsertLockPreemptedFault.CODE)
public class InsertLockPreemptedFault extends BaseTalariaFault
{
  public static final InsertLockPreemptedFault INSTANCE = new InsertLockPreemptedFault();
  static final String CODE = "InsertLockPreempted";


  InsertLockPreemptedFault()
  {
    super(
        CODE,
        "Insert lock preempted while trying to ingest the data."
        + " This can occur if there are higher priority jobs like real-time ingestion running on same time chunks."
    );
  }

  @JsonCreator
  public static InsertLockPreemptedFault instance()
  {
    return INSTANCE;
  }
}
