/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.query.lookup;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.inject.Inject;
import io.imply.druid.util.CloseableNaivePool;
import io.imply.druid.util.Cron;
import io.imply.druid.util.CronFactory;
import org.apache.druid.collections.bitmap.ImmutableBitmap;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.UOE;
import org.apache.druid.java.util.common.concurrent.ScheduledExecutors;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.query.BitmapResultFactory;
import org.apache.druid.query.DefaultBitmapResultFactory;
import org.apache.druid.query.TableDataSource;
import org.apache.druid.query.lookup.LookupExtractor;
import org.apache.druid.query.lookup.LookupExtractorFactory;
import org.apache.druid.query.lookup.LookupIntrospectHandler;
import org.apache.druid.query.planning.DataSourceAnalysis;
import org.apache.druid.segment.QueryableIndex;
import org.apache.druid.segment.ReferenceCountingSegment;
import org.apache.druid.segment.column.BaseColumn;
import org.apache.druid.segment.column.ColumnHolder;
import org.apache.druid.segment.column.ColumnIndexSupplier;
import org.apache.druid.segment.column.DictionaryEncodedColumn;
import org.apache.druid.segment.column.StringValueSetIndex;
import org.apache.druid.server.SegmentManager;
import org.apache.druid.timeline.TimelineObjectHolder;
import org.apache.druid.timeline.VersionedIntervalTimeline;
import org.joda.time.Duration;
import org.roaringbitmap.IntIterator;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.Closeable;
import java.io.IOException;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

public class SegmentFilteredLookupExtractorFactory implements LookupExtractorFactory, SpecializableLookup
{
  private static final Logger log = new Logger(SegmentFilteredLookupExtractorFactory.class);

  private final String table;
  private final List<String> filterColumns;
  @Nullable
  private final SegmentManager manager;
  @Nullable
  private final CronFactory cronFactory;
  @Nullable
  private final Cron cron;
  @Nullable
  private final Runnable tableCallback;

  private final AtomicBoolean running = new AtomicBoolean(false);
  private final AtomicReference<SegRefHolder> segRef = new AtomicReference<>(null);

  @Inject
  public SegmentFilteredLookupExtractorFactory(
      @JsonProperty("table") String table,
      @JsonProperty("filterColumns") List<String> filterColumns,
      @JacksonInject @Nullable SegmentManager manager,
      @JacksonInject @Nullable @ImplyLookup CronFactory cronFactory
  )
  {
    this.table = table;
    this.filterColumns = filterColumns;
    this.manager = manager;
    this.cronFactory = cronFactory;
    if (cronFactory == null) {
      this.cron = null;
      this.tableCallback = null;
    } else {
      this.cron = cronFactory.make(new SegmentReferenceUpdatingCallable());
      this.tableCallback = cron::submitNow;
    }
  }

  @Override
  public boolean start()
  {
    if (manager == null || cron == null) {
      log.info("Cannot start because manager[%s] or cron[%s] is null.", manager, cron);
      return false;
    }

    if (running.compareAndSet(false, true)) {
      cron.scheduleWithFixedDelay(Duration.ZERO, Duration.standardMinutes(1));
      if (manager instanceof SegmentManagerWithCallbacks) {
        log.info("Registering segment load callback for table[%s]", table);
        ((SegmentManagerWithCallbacks) manager).registerCallback(table, tableCallback);
      }
    }

    return true;
  }

  @Override
  public boolean close()
  {
    running.set(false);
    if (manager instanceof SegmentManagerWithCallbacks) {
      log.info("Unregistering segment load callback for table[%s]", table);
      ((SegmentManagerWithCallbacks) manager).unregisterCallback(table, tableCallback);
    }
    if (cron != null) {
      // Run the update logic so that we notice that we were stopped and clean up state as fast as possible
      cron.submitNow();
    }
    return true;
  }

  @Override
  public boolean replaces(@Nullable LookupExtractorFactory other)
  {
    throw new UnsupportedOperationException();
  }

  @Nullable
  @Override
  public LookupIntrospectHandler getIntrospectHandler()
  {
    return null;
  }

  @Override
  public SegmentFilteredLookupExtractor get()
  {
    throw new UOE("lookup over table [%s] requires specialization, cannot use directly.", table);
  }

  @Override
  public LookupExtractorFactory specialize(LookupSpec spec)
  {
    final String[] specParts = spec.getValInBrackets().split("]\\[");
    if (specParts.length < 2) {
      throw new IAE(
          "Lookup [%s] definition incomplete, usage: [%s[<key_column>][<value_column>][<optional_filter_columns...>]]",
          spec.getLookupName(),
          spec.getLookupName()
      );
    }
    if (specParts.length - 2 != filterColumns.size()) {
      throw new IAE(
          "Lookup [%s] expects filters on columns [%s], but only received [%d] values for filters",
          spec.getLookupName(),
          filterColumns,
          specParts.length - 2
      );
    }

    final SegRefHolder segRefHolder = segRef.get();
    // Technically, this null check could happen before the validation, but then we get into a world where someone
    // might think that their query is good just because the segment hasn't been loaded yet.  So, we do the input
    // validation first and then the null check.
    if (segRefHolder == null) {
      // This means that the segment hasn't been loaded yet.  This could be because it isn't actually configured
      // for broadcast *or* it could be because of a race at startup time (expected to be seen in realtime nodes most).
      // The options we have are to
      // 1) throw an exception here (which will break all queries any time the race occurs, which could be on every
      //    start of a realtime node, i.e. a really bad experience)
      // 2) Return an effectively null lookup, which allows queries to make progress, but the result of the lookup
      //    will be null until the data is loaded and then become a real value.  This can cause some weird behavior
      //    but given that it's primarily happening on the stream, it should only impact the most recent data.  This
      //    is the option that we implement here.  This can quite adversely affect lookups at ingestion time (i.e. as
      //    a transform) because the lie (returning null) will actually be persisted instead of ephemeral.  As such,
      //    as long as this is the choice that we make, these lookups should not be used in transform specs for
      //    ingestion.
      // 3) We could go into a sleep-wait loop effectively blocking until the data has been loaded.  This is quite
      //    dangerous as there are mutiple places that end up calling this method and arbitrarily blocking all of them
      //    could lead to unintended consequences.
      //
      // We need the ability for nodes to identify which segments they need at startup time (e.g. an endpoint on the
      // coordinator that could tell a node that it needs XYZ broadcast segments), which would allow us to startup with
      // a bit more reassurance that we have all of the necessary segments already loaded.  Once we had that in place
      // it would likely become safe to switch the treatment here to (1) as then the lack of loading of the segment is
      // indicative of something much more nefarious and unexpected.
      return new NullLookupExtractorFactory(spec);
    }

    final String keyColumn = specParts[0];
    final String valueColumn = specParts[1];
    final ImmutableBitmap filterBitmap;

    try (CloseableNaivePool.PooledResourceHolder<SegmentLookupHolder> holder = segRefHolder.getPool().take()) {
      final SegmentLookupHolder lookupHolder = holder.get();
      QueryableIndex index = lookupHolder.getIndex();
      if (lookupHolder.getDictionaryColumn(keyColumn) == null) {
        throw new IAE("lookup [%s] cannot use column [%s] for key.", spec.getLookupName(), keyColumn);
      }
      if (lookupHolder.getDictionaryColumn(valueColumn) == null) {
        throw new IAE("lookup [%s] cannot use column [%s] for value.", spec.getLookupName(), valueColumn);
      }


      if (filterColumns.size() > 0) {
        final DefaultBitmapResultFactory bitmapResultFactory = new DefaultBitmapResultFactory(index.getBitmapFactoryForDimensions());

        ImmutableBitmap filterBitmapM = null;
        for (int i = 0; i < filterColumns.size(); ++i) {
          final StringValueSetIndex dictIndex = lookupHolder.getAsStringValueSetIndex(filterColumns.get(i));
          final String specPartValue = specParts[2 + i];
          if (filterBitmapM == null) {
            filterBitmapM = dictIndex.forValue(specPartValue).computeBitmapResult(bitmapResultFactory);
          } else {
            filterBitmapM = filterBitmapM.intersection(
                dictIndex.forValue(specPartValue).computeBitmapResult(bitmapResultFactory)
            );
          }
        }
        filterBitmap = filterBitmapM;
      } else {
        filterBitmap = null;
      }
    }

    return new LookupExtractorFactory()
    {
      @Override
      public boolean start()
      {
        throw new UnsupportedOperationException();
      }

      @Override
      public boolean close()
      {
        throw new UnsupportedOperationException();
      }

      @Override
      public boolean replaces(@Nullable LookupExtractorFactory other)
      {
        throw new UnsupportedOperationException();
      }

      @Nullable
      @Override
      public LookupIntrospectHandler getIntrospectHandler()
      {
        throw new UnsupportedOperationException();
      }

      @Override
      public LookupExtractor get()
      {
        return new SegmentFilteredLookupExtractor(segRefHolder, keyColumn, valueColumn, filterBitmap);
      }
    };
  }

  public class SegRefHolder
  {
    private final String version;
    private final ReferenceCountingSegment segment;

    private final CloseableNaivePool<SegmentLookupHolder> pool;

    public SegRefHolder(
        String version,
        ReferenceCountingSegment segment
    )
    {
      this.version = version;
      this.segment = segment;

      this.pool = new CloseableNaivePool<>(
          StringUtils.format("lookup-table[%s]-version[%s]", table, version),
          () -> new SegmentLookupHolder(segment.asQueryableIndex())
      );
    }

    public String getVersion()
    {
      return version;
    }

    public CloseableNaivePool<SegmentLookupHolder> getPool()
    {
      return pool;
    }

    public void close()
    {
      pool.close();
      segment.close();
    }
  }

  public static class SegmentLookupHolder implements AutoCloseable, Closeable
  {
    private final QueryableIndex index;

    private final LinkedHashMap<String, BaseColumn> columns;
    private final Closer closer;

    public SegmentLookupHolder(
        QueryableIndex index
    )
    {
      this.index = index;

      this.columns = new LinkedHashMap<>();
      this.closer = Closer.create();
    }

    public QueryableIndex getIndex()
    {
      return index;
    }

    public BitmapResultFactory<ImmutableBitmap> getBitmapResultFactory()
    {
      return new DefaultBitmapResultFactory(index.getBitmapFactoryForDimensions());
    }

    public StringValueSetIndex getAsStringValueSetIndex(String keyColumn)
    {
      final ColumnHolder columnHolder = index.getColumnHolder(keyColumn);
      if (columnHolder == null) {
        throw new ISE("Column [%s] does not exist, cannot be used in lookup.", keyColumn);
      }
      final ColumnIndexSupplier indexSupplier = columnHolder.getIndexSupplier();
      if (indexSupplier == null) {
        throw new ISE("Column [%s] does not have indexes, cannot be used in lookup.", keyColumn);
      }
      final StringValueSetIndex retVal = indexSupplier.as(StringValueSetIndex.class);
      if (retVal == null) {
        throw new ISE("Column [%s] is not indexed for string values, cannot be used in lookup", keyColumn);
      }
      return retVal;
    }

    @Nullable
    public DictionaryEncodedColumn<String> getDictionaryColumn(String valueColumn)
    {
      final BaseColumn retVal;
      if (columns.containsKey(valueColumn)) {
        retVal = columns.get(valueColumn);
      } else {
        final ColumnHolder colHolder = index.getColumnHolder(valueColumn);
        if (colHolder == null) {
          return null;
        } else {
          retVal = closer.register(colHolder.getColumn());
          columns.put(valueColumn, retVal);
        }
      }

      if (retVal instanceof DictionaryEncodedColumn) {
        //noinspection unchecked
        return (DictionaryEncodedColumn<String>) retVal;
      }

      throw new ISE(
          "column [%s] is not dictionary encoded [%s], cannot be used in lookup.",
          valueColumn,
          retVal.getClass()
      );
    }

    @Override
    public void close() throws IOException
    {
      closer.close();
    }
  }

  private class SegmentFilteredLookupExtractor extends LookupExtractor
  {
    private final SegRefHolder segRef;
    private final String keyColumn;
    private final String valueColumn;
    private final ImmutableBitmap filterBitmap;

    private SegmentFilteredLookupExtractor(
        SegRefHolder segRef,
        String keyColumn,
        String valueColumn,
        ImmutableBitmap filterBitmap
    )
    {
      this.segRef = segRef;
      this.keyColumn = keyColumn;
      this.valueColumn = valueColumn;
      this.filterBitmap = filterBitmap;
    }

    @Nullable
    @Override
    public String apply(@Nullable String key)
    {
      if (segRef == null) {
        return null;
      }

      // It is rather inefficient to be returning this to the pool on each-and-every apply, but lifecycle management
      // isn't built into the LookupExtractor interface, so this is the best that we can do for now...
      try (CloseableNaivePool.PooledResourceHolder<SegmentLookupHolder> holder = segRef.getPool().take()) {
        final SegmentLookupHolder lookupHolder = holder.get();

        final BitmapResultFactory<ImmutableBitmap> bitmapResultFactory = lookupHolder.getBitmapResultFactory();
        final StringValueSetIndex keyColumnIndex = lookupHolder.getAsStringValueSetIndex(keyColumn);
        final DictionaryEncodedColumn<String> valueCol = lookupHolder.getDictionaryColumn(valueColumn);

        ImmutableBitmap bitmap = keyColumnIndex.forValue(key).computeBitmapResult(bitmapResultFactory);
        if (bitmap.isEmpty()) {
          return null;
        }

        if (filterBitmap != null) {
          bitmap = bitmap.intersection(filterBitmap);
        }

        final IntIterator iter = bitmap.iterator();
        if (!iter.hasNext()) {
          return null;
        }

        int rowId = iter.next();
        if (iter.hasNext()) {
          throw new IAE("Non-unique lookup [%s].", table);
        }

        // valueCol should never be null because it will have been validated before this LookupExtractor was
        // created
        //noinspection ConstantConditions
        return valueCol.lookupName(valueCol.getSingleValueRow(rowId));
      }
    }

    @Override
    public List<String> unapply(@Nullable String value)
    {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean canIterate()
    {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean canGetKeySet()
    {
      throw new UnsupportedOperationException();
    }

    @Override
    public Iterable<Map.Entry<String, String>> iterable()
    {
      throw new UnsupportedOperationException();
    }

    @Override
    public Set<String> keySet()
    {
      throw new UnsupportedOperationException();
    }

    @Override
    public byte[] getCacheKey()
    {
      throw new UnsupportedOperationException();
    }
  }

  private class NullLookupExtractorFactory implements LookupExtractorFactory
  {
    private final LookupSpec spec;

    public NullLookupExtractorFactory(LookupSpec spec)
    {
      this.spec = spec;
    }

    @Override
    public boolean start()
    {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean close()
    {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean replaces(@Nullable LookupExtractorFactory other)
    {
      throw new UnsupportedOperationException();
    }

    @Nullable
    @Override
    public LookupIntrospectHandler getIntrospectHandler()
    {
      throw new UnsupportedOperationException();
    }

    @Override
    public LookupExtractor get()
    {
      return new LookupExtractor()
      {
        @Nullable
        @Override
        public String apply(@Nullable String key)
        {
          return null;
        }

        @Override
        public List<String> unapply(@Nullable String value)
        {
          return Collections.emptyList();
        }

        @Override
        public boolean canIterate()
        {
          return true;
        }

        @Override
        public boolean canGetKeySet()
        {
          return true;
        }

        @Override
        public Iterable<Map.Entry<String, String>> iterable()
        {
          // This is used by the introspection endpoint, so throwing an exception here that the table hasn't
          // been loaded can actually be useful;
          throw new ISE("Table [%s] not loaded for lookup [%s]", table, spec.getLookupName());
        }

        @Override
        public Set<String> keySet()
        {
          // This is used by the introspection endpoint, so throwing an exception here that the table hasn't
          // been loaded can actually be useful;
          throw new ISE("Table [%s] not loaded for lookup [%s]", table, spec.getLookupName());
        }

        @Override
        public byte[] getCacheKey()
        {
          // not called
          throw new UnsupportedOperationException();
        }
      };
    }
  }

  private class SegmentReferenceUpdatingCallable implements Callable<ScheduledExecutors.Signal>
  {
    @Override
    public ScheduledExecutors.Signal call()
    {
      if (!running.get()) {
        emptySegRefAndContinue();
        return ScheduledExecutors.Signal.STOP;
      }


      // We have to build a dummy DataSourceAnalysis because the getTimeline API for manager requires it.
      // It would be great if SegmentManager had a method that just took a table name, but it doesn't currently
      // and we are in an extension, so until that day comes, we build this fake object.
      final DataSourceAnalysis analysis = new TableDataSource(table).getAnalysis();

      // manager will never be null because this callable will not be built if it is null
      @SuppressWarnings("ConstantConditions")
      final VersionedIntervalTimeline<String, ReferenceCountingSegment> timeline = manager.getTimeline(analysis)
                                                                                          .orElse(null);
      if (timeline == null) {
        log.info("Unable to load timeline for table [%s]", table);
        return emptySegRefAndContinue();
      }

      final int objs = timeline.getNumObjects();
      if (objs > 10) {
        // This is a pre-emptive check.  Given the assumption that there should only be a single segment
        // at a time, then there would need to be 11 active versions for this check to be exceeded.  But, if
        // a table is used that just generally has lots of segments, this early-exit avoids doing an ETERNITY
        // timeline lookup.
        log.info("timeline for table [%s] had too many objects [%s], cannot load lookup.", table, objs);
        return emptySegRefAndContinue();
      } else {
        final List<TimelineObjectHolder<String, ReferenceCountingSegment>> segs = timeline.lookup(Intervals.ETERNITY);
        if (segs.size() == 1) {
          SegRefHolder old = segRef.get();

          final TimelineObjectHolder<String, ReferenceCountingSegment> holder = segs.get(0);
          if (old != null && Objects.equals(old.getVersion(), holder.getVersion())) {
            return ScheduledExecutors.Signal.REPEAT;
          }

          ReferenceCountingSegment seg = null;
          for (ReferenceCountingSegment payload : holder.getObject().payloads()) {
            if (seg == null) {
              seg = payload;
            } else {
              log.info("table [%s] had more than a single segment, cannot use for lookup", table);
              return emptySegRefAndContinue();
            }
          }

          if (seg == null) {
            log.info("table [%s] was still null after going through segments!?  Should not be possible", table);
            return emptySegRefAndContinue();
          }

          return updateSegRefAndContinue(holder.getVersion(), seg);
        } else {
          log.info("table [%s] has wrong number of segments [%,d] for use by lookup", table, segs.size());
          return emptySegRefAndContinue();
        }
      }
    }

    private ScheduledExecutors.Signal emptySegRefAndContinue()
    {
      closeOld(segRef.getAndSet(null));
      return ScheduledExecutors.Signal.REPEAT;
    }

    @Nonnull
    private ScheduledExecutors.Signal updateSegRefAndContinue(String version, ReferenceCountingSegment seg)
    {
      if (!seg.increment()) {
        return emptySegRefAndContinue();
      }

      closeOld(segRef.getAndUpdate(segRefHolder -> {
        if (segRefHolder == null) {
          log.info("Lookup for table [%s] loaded first segment with version [%s].", table, version);
        } else {
          log.info(
              "Lookup for table [%s] replaced old segment [%s] with new version [%s]",
              table,
              segRefHolder.getVersion(),
              version
          );
        }
        return new SegRefHolder(version, seg);
      }));
      return ScheduledExecutors.Signal.REPEAT;
    }

    private void closeOld(SegRefHolder oldVal)
    {
      if (oldVal != null && cronFactory != null) {
        // Delay the clean-up for 10 seconds to avoid races between usages and cleanup.
        cronFactory.scheduleOnce(oldVal::close, Duration.standardSeconds(10));
      }
    }
  }
}
