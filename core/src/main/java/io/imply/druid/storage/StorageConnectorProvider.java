/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.storage;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.google.inject.Provider;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
public interface StorageConnectorProvider extends Provider<StorageConnector>
{
}
