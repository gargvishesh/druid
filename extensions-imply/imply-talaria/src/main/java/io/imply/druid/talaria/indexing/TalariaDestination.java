/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.talaria.indexing;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
@JsonSubTypes(value = {
    @JsonSubTypes.Type(name = DataSourceTalariaDestination.TYPE, value = DataSourceTalariaDestination.class),
    @JsonSubTypes.Type(name = ExternalTalariaDestination.TYPE, value = ExternalTalariaDestination.class),
    @JsonSubTypes.Type(name = TaskReportTalariaDestination.TYPE, value = TaskReportTalariaDestination.class)
})
public interface TalariaDestination
{
  // No methods. Just a marker interface for deserialization.
}
