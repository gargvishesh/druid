/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.talaria.querykit;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.google.common.primitives.Ints;
import io.imply.druid.talaria.indexing.InputChannels;
import org.apache.druid.data.input.InputFormat;
import org.apache.druid.data.input.InputSource;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.segment.column.RowSignature;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Objects;

public class QueryWorkerInputSpec
{
  private final QueryWorkerInputType inputType;
  private final InputSource inputSource;
  private final InputFormat inputFormat;
  private final RowSignature signature;
  private final List<DataSegmentWithInterval> segments;
  private final Integer stageNumber;

  @JsonCreator
  private QueryWorkerInputSpec(
      @JsonProperty("type") final QueryWorkerInputType inputType,
      @JsonProperty("inputSource") final InputSource inputSource,
      @JsonProperty("inputFormat") final InputFormat inputFormat,
      @JsonProperty("signature") final RowSignature signature,
      @JsonProperty("segments") final List<DataSegmentWithInterval> segments,
      @JsonProperty("stageNumber") final Integer stageNumber
  )
  {
    this.inputType = Preconditions.checkNotNull(inputType, "inputType");
    this.inputSource = inputSource;
    this.inputFormat = inputFormat;
    this.signature = signature;
    this.segments = segments;
    this.stageNumber = stageNumber;

    switch (inputType) {
      case TABLE:
        Preconditions.checkNotNull(segments, "segments");
        if (inputFormat != null || inputSource != null || signature != null || stageNumber != null) {
          throw new ISE(
              "Cannot provide 'inputFormat', 'inputSource', 'signature', or 'stageNumber' with type [%s]",
              inputType
          );
        }
        break;

      case EXTERNAL:
        Preconditions.checkNotNull(inputFormat, "inputFormat");
        Preconditions.checkNotNull(inputSource, "inputSource");
        Preconditions.checkNotNull(signature, "signature");
        if (segments != null || stageNumber != null) {
          throw new ISE("Cannot provide 'segments' or 'stageNumber' with type [%s]", inputType);
        }
        break;

      case SUBQUERY:
        Preconditions.checkNotNull(stageNumber, "stageNumber");
        if (inputFormat != null || inputSource != null || signature != null || segments != null) {
          throw new ISE("Cannot provide any parameters with type [%s]", inputType);
        }
        break;

      default:
        throw new IAE("Unsupported type [%s]", inputType);
    }
  }

  public static QueryWorkerInputSpec forTable(final List<DataSegmentWithInterval> segments)
  {
    return new QueryWorkerInputSpec(QueryWorkerInputType.TABLE, null, null, null, segments, null);
  }

  public static QueryWorkerInputSpec forInputSource(
      final InputSource inputSource,
      final InputFormat inputFormat,
      final RowSignature signature
  )
  {
    return new QueryWorkerInputSpec(QueryWorkerInputType.EXTERNAL, inputSource, inputFormat, signature, null, null);
  }

  public static QueryWorkerInputSpec forSubQuery(final int stageNumber)
  {
    return new QueryWorkerInputSpec(QueryWorkerInputType.SUBQUERY, null, null, null, null, stageNumber);
  }

  @JsonProperty("type")
  public QueryWorkerInputType type()
  {
    return inputType;
  }

  @Nullable
  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public InputSource getInputSource()
  {
    return inputSource;
  }

  @Nullable
  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public InputFormat getInputFormat()
  {
    return inputFormat;
  }

  @Nullable
  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public RowSignature getSignature()
  {
    return signature;
  }

  @Nullable
  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public List<DataSegmentWithInterval> getSegments()
  {
    return segments;
  }

  @Nullable
  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public Integer getStageNumber()
  {
    return stageNumber;
  }

  public int computeNumProcessors(final InputChannels inputChannels)
  {
    if (inputType == QueryWorkerInputType.SUBQUERY) {
      return Ints.checkedCast(
          inputChannels.getStagePartitions()
                       .stream()
                       .filter(stagePartition -> stagePartition.getStageId().getStageNumber() == stageNumber)
                       .count()
      );
    } else if (inputType == QueryWorkerInputType.TABLE) {
      return segments.size();
    } else {
      // TODO(gianm): Potentially do further splitting of the InputSource
      assert inputType == QueryWorkerInputType.EXTERNAL;
      return 1;
    }
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    QueryWorkerInputSpec that = (QueryWorkerInputSpec) o;
    return Objects.equals(inputSource, that.inputSource)
           && Objects.equals(inputFormat, that.inputFormat)
           && Objects.equals(signature, that.signature)
           && Objects.equals(segments, that.segments)
           && Objects.equals(stageNumber, that.stageNumber);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(inputSource, inputFormat, signature, segments, stageNumber);
  }

  @Override
  public String toString()
  {
    if (segments != null) {
      return "QueryWorkerInput{" +
             "segments=" + segments +
             '}';
    } else if (inputSource != null) {
      return "QueryWorkerInput{" +
             "inputSource=" + inputSource +
             ", inputFormat=" + inputFormat +
             ", signature=" + signature +
             '}';
    } else {
      return "QueryWorkerInput{" +
             "stageNumber=" + stageNumber +
             '}';
    }
  }
}
