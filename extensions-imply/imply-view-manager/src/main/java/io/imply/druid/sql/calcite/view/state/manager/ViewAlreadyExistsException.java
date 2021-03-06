/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.sql.calcite.view.state.manager;

import org.apache.druid.java.util.common.StringUtils;

public class ViewAlreadyExistsException extends RuntimeException
{
  public ViewAlreadyExistsException(String name, String namespace)
  {
    super(StringUtils.format("View %s.%s already exists", namespace, name));
  }

  public ViewAlreadyExistsException(String name)
  {
    super(StringUtils.format("View %s already exists", name));
  }
}
