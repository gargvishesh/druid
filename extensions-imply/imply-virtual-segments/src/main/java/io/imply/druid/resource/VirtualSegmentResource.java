/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.resource;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.imply.druid.loading.VirtualSegmentLoader;
import io.imply.druid.segment.VirtualReferenceCountingSegment;
import org.apache.druid.guice.annotations.Json;
import org.apache.druid.java.util.emitter.EmittingLogger;
import org.apache.druid.query.TableDataSource;
import org.apache.druid.query.planning.DataSourceAnalysis;
import org.apache.druid.segment.ReferenceCountingSegment;
import org.apache.druid.server.SegmentManager;
import org.apache.druid.timeline.VersionedIntervalTimeline;
import org.apache.http.annotation.Experimental;

import javax.inject.Inject;
import javax.ws.rs.DELETE;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

@Experimental
@Path("/druid/historical/v1/virtualsegments/")
public class VirtualSegmentResource
{
  protected static final EmittingLogger log = new EmittingLogger(VirtualSegmentResource.class);
  private final SegmentManager segmentManger;
  private final VirtualSegmentLoader virtualSegmentLoader;
  private final ObjectMapper objectMapper;

  @Inject
  public VirtualSegmentResource(
      SegmentManager segmentManager,
      VirtualSegmentLoader virtualSegmentLoader,
      @Json ObjectMapper objectMapper
  )
  {
    this.segmentManger = segmentManager;
    this.virtualSegmentLoader = virtualSegmentLoader;
    this.objectMapper = objectMapper;
  }

  /**
   * Internal API used for evicting segments. API's are enabled only when
   * {@link io.imply.druid.VirtualSegmentModule#EXPERIMENTAL_PROPERTY} is set to true.
   *
   * @returns Map<DataSourceName, List < SegmentID> evicted in json format
   */
  @DELETE
  @Produces(MediaType.APPLICATION_JSON)
  public Response evictVirtualSegments() throws JsonProcessingException
  {
    final Map<String, List<String>> dataSourceSegment = new HashMap<>();
    segmentManger.getDataSourceNames().forEach(
        dataSource -> {
          final Optional<VersionedIntervalTimeline<String, ReferenceCountingSegment>> maybeTimeline =
              segmentManger.getTimeline(DataSourceAnalysis.forDataSource(TableDataSource.create(dataSource)));

          if (maybeTimeline.isPresent()) {

            for (ReferenceCountingSegment referenceCountingSegment : maybeTimeline.get().iterateAllObjects()) {
              //TODO: there can be a case where numReferences changes to non zero value after the check
              if (referenceCountingSegment.getNumReferences() == 0) {
                log.info("Removing virtual segment %s", referenceCountingSegment.getId().toString());
                dataSourceSegment.computeIfAbsent(dataSource, k -> new ArrayList<>());
                dataSourceSegment.get(dataSource).add(referenceCountingSegment.getId().toString());
                virtualSegmentLoader.evictSegment((VirtualReferenceCountingSegment) referenceCountingSegment);
              }
            }
          }
        });

    return Response.status(Response.Status.ACCEPTED)
                   .type(MediaType.APPLICATION_JSON_TYPE)
                   .entity(objectMapper.writeValueAsString(dataSourceSegment))
                   .build();
  }

}
