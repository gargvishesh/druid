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

package org.apache.druid.indexing.common.task;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.druid.indexer.CompactionEngine;
import org.apache.druid.indexer.TaskStatus;
import org.apache.druid.indexing.common.TaskToolbox;
import org.apache.druid.java.util.common.NonnullPair;
import org.apache.druid.segment.indexing.DataSchema;
import org.joda.time.Interval;

import java.util.List;

/**
 * Strategy to be used for executing a compaction task.
 * All subtypes should be synchronized with {@link CompactionEngine}.
 */
@JsonTypeInfo(
    use = JsonTypeInfo.Id.NAME,
    include = JsonTypeInfo.As.EXISTING_PROPERTY,
    property = CompactionStrategy.TYPE_PROPERTY,
    visible = true)
@JsonSubTypes(value = {
    @JsonSubTypes.Type(name = NativeCompactionStrategy.type, value = NativeCompactionStrategy.class)
})
public interface CompactionStrategy
{
  String TYPE_PROPERTY = "type";

  TaskStatus runCompactionTasks(
      CompactionTask compactionTask,
      TaskToolbox taskToolbox,
      List<NonnullPair<Interval, DataSchema>> dataSchemas
  ) throws JsonProcessingException;

  @JsonProperty
  String getType();
}
