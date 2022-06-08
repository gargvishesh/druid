/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.talaria.frame;

import org.apache.druid.java.util.common.ISE;

import javax.annotation.Nullable;

public enum FrameType
{
  COLUMNAR((byte) 0x11),
  ROW_BASED((byte) 0x12);

  /**
   * Returns the frame type for a particular version byte, or null if the version byte is unrecognized.
   */
  @Nullable
  public static FrameType forVersion(final byte versionByte)
  {
    for (final FrameType type : values()) {
      if (type.version() == versionByte) {
        return type;
      }
    }

    return null;
  }

  private final byte versionByte;

  FrameType(final byte magicByte)
  {
    this.versionByte = magicByte;
  }

  public byte version()
  {
    return versionByte;
  }

  public Frame ensureType(final Frame frame)
  {
    if (frame == null) {
      throw new NullPointerException("Null frame");
    }

    if (frame.type() != this) {
      throw new ISE("Frame type must be [%s], but was [%s]", this, frame.type());
    }

    return frame;
  }
}
