/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.query.samplinggroupby;

import com.google.common.base.Preconditions;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.guava.Yielder;
import org.apache.druid.java.util.common.guava.Yielders;
import org.apache.druid.java.util.common.guava.YieldingAccumulator;
import org.apache.druid.java.util.common.guava.YieldingSequenceBase;
import org.apache.druid.query.QueryPlus;
import org.apache.druid.query.groupby.ResultRow;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

/**
 * The input sequence received by this yielder is de-duplicated and sorted on the hashes of the groups.
 * We pick the first maxGroups groups with lowest hashes in this yieler per time grain. The final theta of the result
 * per grain is either the hash value of the (maxGroups + 1)th group or the minimum theta of the result groups if they
 * are less than maxGroups.
 */
public class SamplingGroupByMergingSequence extends YieldingSequenceBase<ResultRow>
{
  private final QueryPlus<ResultRow> queryPlus;
  private final Sequence<ResultRow> sequence;
  private Yielder<ResultRow> baseYielder;

  public SamplingGroupByMergingSequence(
      QueryPlus<ResultRow> queryPlus,
      Sequence<ResultRow> sequence
  )
  {
    this.queryPlus = Preconditions.checkNotNull(queryPlus, "queryPlus is null");
    this.sequence = Preconditions.checkNotNull(sequence, "queryRunner is null");
  }

  @Override
  public <OutType> Yielder<OutType> toYielder(
      OutType initValue,
      YieldingAccumulator<OutType, ResultRow> accumulator
  )
  {
    // get a yielder which which yields one element of the sequence at every accumulate call
    this.baseYielder = Yielders.each(sequence);
    return makeYielder(
        accumulator,
        initValue,
        ResultRowIteratorWithTheta.EMPTY
    );
  }

  private <OutType> Yielder<OutType> makeYielder(
      YieldingAccumulator<OutType, ResultRow> accumulator,
      OutType initValue,
      ResultRowIteratorWithTheta currentGrainResultsIteratorWithTheta
  )
  {
    if (baseYielder.isDone() && !currentGrainResultsIteratorWithTheta.getResultRowIterator().hasNext()) {
      return Yielders.done(initValue, baseYielder);
    }

    SamplingGroupByQuery query = (SamplingGroupByQuery) queryPlus.getQuery();
    int thetaColIdx = query.getIntermediateResultRowThetaColumnIndex();
    int hashColIdx = query.getIntermediateResultRowHashColumnIndex();
    if (!currentGrainResultsIteratorWithTheta.getResultRowIterator().hasNext()) {
      currentGrainResultsIteratorWithTheta = fetchResultRowIteratorWithThetaForNewTimeGrain(
          query,
          hashColIdx,
          thetaColIdx
      );
    }

    Iterator<ResultRow> currentGrainResultsIterator = currentGrainResultsIteratorWithTheta.getResultRowIterator();
    OutType retval = initValue;
    while (!accumulator.yielded()) {
      if (currentGrainResultsIterator.hasNext()) {
        ResultRow group = currentGrainResultsIterator.next();
        group.set(thetaColIdx, currentGrainResultsIteratorWithTheta.getTheta());
        retval = accumulator.accumulate(retval, group);
      } else if (!baseYielder.isDone()) {
        return makeYielder(
            accumulator,
            retval,
            ResultRowIteratorWithTheta.EMPTY
        );
      } else {
        break; // the base yielder doesn't have any new grain and the current grain iterator is finished. we're done.
      }
    }

    OutType finalRetval = retval;
    ResultRowIteratorWithTheta finalCurrentGrainResultsIteratorWithTheta = currentGrainResultsIteratorWithTheta;
    return new Yielder<OutType>()
    {
      @Override
      public OutType get()
      {
        return finalRetval;
      }

      @Override
      public Yielder<OutType> next(OutType initValue)
      {
        if (baseYielder.isDone() && !finalCurrentGrainResultsIteratorWithTheta.getResultRowIterator().hasNext()) {
          return Yielders.done(initValue, baseYielder);
        }
        accumulator.reset();
        if (finalCurrentGrainResultsIteratorWithTheta.getResultRowIterator().hasNext()) {
          return makeYielder(
              accumulator,
              initValue,
              finalCurrentGrainResultsIteratorWithTheta
          );
        } else {
          return makeYielder(
              accumulator,
              initValue,
              ResultRowIteratorWithTheta.EMPTY
          );
        }
      }

      @Override
      public boolean isDone()
      {
        // This is false because we are sending an object in this yielder object.
        // As per yielder semantics, done() yielder's results aren't considered unless the accumulator has never yielded.
        return false;
      }

      @Override
      public void close() throws IOException
      {
        baseYielder.close();
      }
    };
  }

  /**
   * Fetches the result row iterator for the current time grain along with the theta for it. The iterator is derived
   * from the base yielder which is sorted on time first and then the grouping dimensions.
   * @param query the {@link SamplingGroupByQuery} query
   * @param hashColIdx the index of hash column in the result row object array
   * @param thetaColIdx the index of theta column in the result row object array
   * @return a holder object with an iterator for results for the current grain and the theta result
   */
  private ResultRowIteratorWithTheta fetchResultRowIteratorWithThetaForNewTimeGrain(
      SamplingGroupByQuery query,
      int hashColIdx,
      int thetaColIdx
  )
  {
    long currMinTheta = Long.MAX_VALUE; // sketch uses LONG.MAX_VALUE as the init theta
    long prevTS = -1;
    List<ResultRow> currentGrainResultsBuilder = new ArrayList<>();
    boolean drainingCurrGrain = false;
    while (!baseYielder.isDone()) {
      ResultRow resultRow = baseYielder.get();
      if (query.isIntermediateResultRowWithTimestamp()) { // traverse a time grain
        long currentTS = resultRow.getLong(0);
        if (prevTS >= 0 && currentTS != prevTS) {
          break;
        }
        prevTS = currentTS;
      }
      long rowHash = resultRow.getLong(hashColIdx);
      if (currentGrainResultsBuilder.size() < query.getMaxGroups()) { // the size needs to be maxGroups
        currentGrainResultsBuilder.add(resultRow);
      } else if (!drainingCurrGrain && currentGrainResultsBuilder.size() == query.getMaxGroups()) {
        currMinTheta = rowHash; // if the size is already maxGroups, don't add the row. instead derive theta from it
        drainingCurrGrain = true;
      }
      if (!drainingCurrGrain) {
        // maintaing this incase the total result groups are less than maxGroups.
        // In that case, the theta is min over all thetas
        currMinTheta = Math.min(resultRow.getLong(thetaColIdx), currMinTheta);
      }
      baseYielder = baseYielder.next(null);
    }
    return new ResultRowIteratorWithTheta(currentGrainResultsBuilder.iterator(), currMinTheta);
  }

  private static class ResultRowIteratorWithTheta
  {
    public static final ResultRowIteratorWithTheta EMPTY =
        new ResultRowIteratorWithTheta(Collections.emptyIterator(), -1);
    private final Iterator<ResultRow> resultRowIterator;
    private final long theta;

    public ResultRowIteratorWithTheta(Iterator<ResultRow> resultRowIterator, long theta)
    {
      this.resultRowIterator = resultRowIterator;
      this.theta = theta;
    }

    public Iterator<ResultRow> getResultRowIterator()
    {
      return resultRowIterator;
    }

    public long getTheta()
    {
      return theta;
    }
  }
}

