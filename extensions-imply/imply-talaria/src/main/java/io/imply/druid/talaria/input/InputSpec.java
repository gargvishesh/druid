/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.talaria.input;

import com.fasterxml.jackson.annotation.JsonTypeInfo;

/**
 * Description of an input. Sliced into {@link InputSlice} by {@link InputSpecSlicer} and then assigned to workers.
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
public interface InputSpec
{
}
