/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.talaria.counters;

import com.fasterxml.jackson.annotation.JsonTypeInfo;

/**
 * Marker interface for the results of {@link QueryCounter#snapshot()}. No methods, because the only purpose of these
 * snapshots is to pass things along from worker -> controller -> report.
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
public interface QueryCounterSnapshot
{
}
