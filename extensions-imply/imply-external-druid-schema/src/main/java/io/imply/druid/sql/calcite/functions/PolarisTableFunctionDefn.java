/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */


package io.imply.druid.sql.calcite.functions;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import org.apache.druid.data.input.impl.LocalInputSource;

/**
 * Used for holding the name of a function, and the parameters that it was called with and their values.
 * This class is used when making RPC calls to Polaris when resolving table functions, by the
 * {@link PolarisTableFunctionResolver}
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = PolarisTableFunctionDefn.TYPE_PROPERTY)
@JsonSubTypes(value = {
    @JsonSubTypes.Type(name = LocalInputSource.TYPE_KEY, value = PolarisSourceOperatorConversion.PolarisSourceFunctionDefn.class)
})
public interface PolarisTableFunctionDefn
{
  String TYPE_PROPERTY = "name";
}
