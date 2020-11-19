/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.ingest.jobs;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import io.imply.druid.ingest.jobs.status.FailedJobStatus;
import io.imply.druid.ingest.jobs.status.TaskBasedJobStatus;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type", defaultImpl = TaskBasedJobStatus.class)
@JsonSubTypes(value = {
    @JsonSubTypes.Type(name = "fail", value = FailedJobStatus.class),
    @JsonSubTypes.Type(name = "task", value = TaskBasedJobStatus.class)
})
public interface JobStatus
{
}
