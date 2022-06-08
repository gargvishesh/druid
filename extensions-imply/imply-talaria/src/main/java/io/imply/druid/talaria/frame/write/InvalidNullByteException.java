/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.talaria.frame.write;

/**
 * Exception thrown by {@link FrameWriterUtils#copyByteBufferToMemory} if configured to check for null bytes
 * and a null byte is encountered.
 */
public class InvalidNullByteException extends RuntimeException
{
}
