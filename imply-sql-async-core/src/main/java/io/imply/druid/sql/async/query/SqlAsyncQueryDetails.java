/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.sql.async.query;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.query.QueryCapacityExceededException;
import org.apache.druid.query.QueryException;
import org.apache.druid.query.QueryInterruptedException;
import org.apache.druid.query.QueryTimeoutException;
import org.apache.druid.query.QueryUnsupportedException;
import org.apache.druid.query.ResourceLimitExceededException;
import org.apache.druid.sql.http.ResultFormat;

import javax.annotation.Nullable;
import java.util.Objects;

@JsonInclude(JsonInclude.Include.NON_NULL)
public class SqlAsyncQueryDetails
{
  private static final long UNKNOWN_RESULT_LENGTH = 0;

  private final String asyncResultId;
  private final State state;
  private final ResultFormat resultFormat;
  private final long resultLength;

  @Nullable // User identity. Can only be null if state == ERROR
  private final String identity;

  @Nullable
  private final QueryException error;

  @JsonCreator
  private SqlAsyncQueryDetails(
      @JsonProperty("asyncResultId") final String asyncResultId,
      @JsonProperty("state") final State state,
      @JsonProperty("identity") @Nullable final String identity,
      @JsonProperty("resultFormat") final ResultFormat resultFormat,
      @JsonProperty("resultLength") final long resultLength,
      @JsonProperty("error") @Nullable final QueryException error
  )
  {
    this.asyncResultId = Preconditions.checkNotNull(asyncResultId, "asyncResultId");
    this.state = Preconditions.checkNotNull(state, "state");
    this.identity = identity;
    this.resultFormat = Preconditions.checkNotNull(resultFormat, "resultFormat");
    this.resultLength = resultLength;
    this.error = error == null ? null : convertException(error);

    if (asyncResultId.isEmpty()) {
      throw new IAE("asyncResultId must be nonempty");
    }

    if (state != State.FAILED && identity == null) {
      throw new ISE("Cannot have nil identity in state [%s]", state);
    }

    if (state != State.FAILED && error != null) {
      throw new ISE("Cannot have error details in state [%s]", state);
    }

    if (state != State.COMPLETE && resultLength != UNKNOWN_RESULT_LENGTH) {
      throw new ISE("Cannot have result length in state [%s]", state);
    }
  }

  public static SqlAsyncQueryDetails createNew(
      final String asyncResultId,
      final String identity,
      final ResultFormat resultFormat
  )
  {
    return new SqlAsyncQueryDetails(
        asyncResultId,
        State.INITIALIZED,
        identity,
        resultFormat,
        UNKNOWN_RESULT_LENGTH,
        null
    );
  }

  public static SqlAsyncQueryDetails createError(
      final String asyncResultId,
      @Nullable final String identity,
      final ResultFormat resultFormat,
      @Nullable final Throwable e
  )
  {
    return new SqlAsyncQueryDetails(
        asyncResultId,
        State.FAILED,
        identity,
        resultFormat,
        UNKNOWN_RESULT_LENGTH,
        // Don't use QueryInterruptedException.wrapIfNeeded, because we want to leave regular QueryException unwrapped
        e instanceof QueryException ? (QueryException) e : new QueryInterruptedException(e)
    );
  }

  @JsonProperty
  public String getAsyncResultId()
  {
    return asyncResultId;
  }

  @JsonProperty
  public ResultFormat getResultFormat()
  {
    return resultFormat;
  }

  @JsonProperty
  public long getResultLength()
  {
    return resultLength;
  }

  @JsonProperty
  public State getState()
  {
    return state;
  }

  @Nullable
  @JsonProperty
  public String getIdentity()
  {
    return identity;
  }

  @Nullable
  @JsonProperty
  public QueryException getError()
  {
    return error;
  }

  public SqlAsyncQueryDetails toRunning()
  {
    return new SqlAsyncQueryDetails(
        asyncResultId,
        State.RUNNING,
        identity,
        resultFormat,
        resultLength,
        error
    );
  }

  public SqlAsyncQueryDetails toComplete(final long newResultLength)
  {
    return new SqlAsyncQueryDetails(
        asyncResultId,
        State.COMPLETE,
        identity,
        resultFormat,
        newResultLength,
        error
    );
  }


  public SqlAsyncQueryDetails toError(@Nullable final Throwable e)
  {
    return createError(asyncResultId, identity, resultFormat, e);
  }

  public SqlAsyncQueryDetails toUndetermined()
  {
    return new SqlAsyncQueryDetails(
        asyncResultId,
        State.UNDETERMINED,
        identity,
        resultFormat,
        resultLength,
        null
    );
  }

  public SqlAsyncQueryDetailsApiResponse toApiResponse()
  {
    return new SqlAsyncQueryDetailsApiResponse(
        asyncResultId,
        state,
        state == State.COMPLETE ? resultFormat : null,
        resultLength,
        error
    );
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
    SqlAsyncQueryDetails that = (SqlAsyncQueryDetails) o;
    boolean equals = resultLength == that.resultLength
                     && Objects.equals(asyncResultId, that.asyncResultId)
                     && state == that.state
                     && resultFormat == that.resultFormat
                     && Objects.equals(identity, that.identity);

    if (!equals) {
      return false;
    }

    if (error == null && that.error == null) {
      return true;
    } else if (error != null && that.error != null) {
      return Objects.equals(error.getErrorClass(), that.error.getErrorClass())
             && Objects.equals(error.getErrorCode(), that.error.getErrorCode())
             && Objects.equals(error.getMessage(), that.error.getMessage())
             && Objects.equals(error.getHost(), that.error.getHost());
    }
    return false;
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(asyncResultId, state, resultFormat, resultLength, identity, error);
  }

  @Override
  public String toString()
  {
    return "SqlAsyncQueryDetails{" +
           "asyncResultId='" + asyncResultId + '\'' +
           ", state=" + state +
           ", resultFormat=" + resultFormat +
           ", resultLength=" + resultLength +
           ", identity='" + identity + '\'' +
           ", error=" + error +
           '}';
  }

  // TODO: should be extendable
  public enum State
  {
    INITIALIZED {
      @Override
      public boolean isFinal()
      {
        return false;
      }
    },
    RUNNING {
      @Override
      public boolean isFinal()
      {
        return false;
      }
    },
    COMPLETE {
      @Override
      public boolean isFinal()
      {
        return true;
      }
    },
    FAILED {
      @Override
      public boolean isFinal()
      {
        return true;
      }
    },
    UNDETERMINED {
      @Override
      public boolean isFinal()
      {
        return true;
      }
    };

    public abstract boolean isFinal();
  }

  // copied over from JsonParserIterator
  private QueryException convertException(Throwable cause)
  {
    if (cause instanceof QueryException) {
      final QueryException queryException = (QueryException) cause;
      if (queryException.getErrorCode() == null) {
        // errorCode should not be null now, but maybe could be null in the past..
        return new QueryInterruptedException(
            queryException.getErrorCode(),
            queryException.getMessage(),
            queryException.getErrorClass(),
            queryException.getHost()
        );
      }

      // Note: this switch clause is to restore the 'type' information of QueryExceptions which is lost during
      // JSON serialization. This is not a good way to restore the correct exception type. Rather, QueryException
      // should store its type when it is serialized, so that we can know the exact type when it is deserialized.
      switch (queryException.getErrorCode()) {
        // The below is the list of exceptions that can be thrown in historicals and propagated to the broker.
        case QueryTimeoutException.ERROR_CODE:
          return new QueryTimeoutException(
              queryException.getErrorCode(),
              queryException.getMessage(),
              queryException.getErrorClass(),
              queryException.getHost()
          );
        case QueryCapacityExceededException.ERROR_CODE:
          return new QueryCapacityExceededException(
              queryException.getErrorCode(),
              queryException.getMessage(),
              queryException.getErrorClass(),
              queryException.getHost()
          );
        case QueryUnsupportedException.ERROR_CODE:
          return new QueryUnsupportedException(
              queryException.getErrorCode(),
              queryException.getMessage(),
              queryException.getErrorClass(),
              queryException.getHost()
          );
        case ResourceLimitExceededException.ERROR_CODE:
          return new ResourceLimitExceededException(
              queryException.getErrorCode(),
              queryException.getMessage(),
              queryException.getErrorClass(),
              queryException.getHost()
          );
        default:
          return new QueryInterruptedException(
              queryException.getErrorCode(),
              queryException.getMessage(),
              queryException.getErrorClass(),
              queryException.getHost()
          );
      }
    } else {
      return new QueryInterruptedException(cause);
    }
  }
}
