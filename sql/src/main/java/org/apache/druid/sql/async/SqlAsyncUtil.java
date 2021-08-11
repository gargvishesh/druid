/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.druid.sql.async;

import com.google.common.hash.Hashing;

import java.nio.charset.StandardCharsets;
import java.util.regex.Pattern;

public class SqlAsyncUtil
{
  // Nothing bad will happen if IDs matching this pattern are used as-is in znodes, filenames, etc.
  private static final Pattern SAFE_ID_PATTERN = Pattern.compile("^[a-zA-Z0-9._\\-]{1,128}$");

  private SqlAsyncUtil()
  {
    // No instantiation.
  }

  public static String safeId(final String id)
  {
    if (SAFE_ID_PATTERN.matcher(id).matches()) {
      return id;
    } else {
      return Hashing.sha256().hashString(id, StandardCharsets.UTF_8).toString();
    }
  }
}
