/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.autoscaling.server;

/**
 * Provides a specialized Exception type for the GCE module
 */
public class ImplyManagerServiceException extends Exception
{
  public ImplyManagerServiceException(String message)
  {
    super(message);
  }

}
