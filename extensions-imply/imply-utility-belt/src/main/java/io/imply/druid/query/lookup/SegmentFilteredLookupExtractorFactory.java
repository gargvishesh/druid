package io.imply.druid.query.lookup;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.inject.Inject;
import io.imply.druid.util.CloseableNaivePool;
import org.apache.druid.collections.bitmap.ImmutableBitmap;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.StringUtils;
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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

public class SegmentFilteredLookupExtractorFactory implements LookupExtractorFactory, SpecializableLookup
{
  private static final Logger log = new Logger(SegmentFilteredLookupExtractorFactory.class);
  private static final ScheduledExecutorService cron = ScheduledExecutors.fixed(1, "SegmentFiltered");

  private final String table;
  private final String keyColumn;
  private final String valueColumn;
  private final List<String> filterColumns;
  @Nullable
  private final SegmentManager manager;

  private final AtomicBoolean running = new AtomicBoolean(false);
  private final AtomicReference<SegRefHolder> segRef = new AtomicReference<>(null);

  @Inject
  public SegmentFilteredLookupExtractorFactory(
      @JsonProperty("table") String table,
      @JsonProperty("keyColumn") String keyColumn,
      @JsonProperty("valueColumn") String valueColumn,
      @JsonProperty("filterColumns") List<String> filterColumns,
      @JacksonInject @Nullable SegmentManager manager
  )
  {
    this.table = table;
    this.keyColumn = keyColumn;
    this.valueColumn = valueColumn;
    this.filterColumns = filterColumns;
    this.manager = manager;
  }

  @Override
  public boolean start()
  {
    if (manager == null) {
      return false;
    }

    if (running.compareAndSet(false, true)) {
      ScheduledExecutors.scheduleWithFixedDelay(
          cron,
          Duration.ZERO,
          Duration.standardMinutes(1),
          new Callable<ScheduledExecutors.Signal>()
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
              final VersionedIntervalTimeline<String, ReferenceCountingSegment> timeline = manager.getTimeline(analysis)
                                                                                                  .orElse(null);
              if (timeline == null) {
                log.info("Unable to load timeline for table [%s]", table);
                return emptySegRefAndContinue();
              }

              final int objs = timeline.getNumObjects();
              if (objs > 10) {
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
              if (oldVal != null) {
                // Delay the clean-up for 10 seconds to avoid races between usages and cleanup.
                cron.schedule(oldVal::close, 10, TimeUnit.SECONDS);
              }
            }
          }
      );
    }

    return true;
  }

  @Override
  public boolean close()
  {
    running.set(false);
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
    return new SegmentFilteredLookupExtractor(segRef.get());
  }

  @Override
  public LookupExtractorFactory specialize(String value)
  {
    final SegRefHolder segRefHolder = segRef.get();
    final ImmutableBitmap filterBitmap;
    if (segRefHolder == null) {
      filterBitmap = null;
    } else {
      final String[] filterValues = value.split("]\\[");
      int numFilters = Math.min(filterValues.length, filterColumns.size());

      try (CloseableNaivePool.PooledResourceHolder<SegmentLookupHolder> holder = segRefHolder.getPool().take()) {
        final SegmentLookupHolder lookupHolder = holder.get();

        QueryableIndex index = lookupHolder.getIndex();
        final DefaultBitmapResultFactory bitmapResultFactory = new DefaultBitmapResultFactory(index.getBitmapFactoryForDimensions());

        ImmutableBitmap filterBitmapM = null;
        for (int i = 0; i < numFilters; ++i) {
          final StringValueSetIndex dictIndex = lookupHolder.getAsStringValueSetIndex(filterColumns.get(i));
          if (filterBitmapM == null) {
            filterBitmapM = dictIndex.forValue(filterValues[i]).computeBitmapResult(bitmapResultFactory);
          } else {
            filterBitmapM = filterBitmapM.intersection(
                dictIndex.forValue(filterValues[i]).computeBitmapResult(bitmapResultFactory)
            );
          }
        }
        filterBitmap = filterBitmapM;
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
        return null;
      }

      @Override
      public LookupExtractor get()
      {
        final SegmentFilteredLookupExtractor lookupExtractor = new SegmentFilteredLookupExtractor(segRefHolder);
        lookupExtractor.filterBitmap = filterBitmap;
        return lookupExtractor;
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

    public DictionaryEncodedColumn<String> getDictionaryColumn(String valueColumn)
    {
      final BaseColumn retVal;
      if (columns.containsKey(valueColumn)) {
        retVal = columns.get(valueColumn);
      } else {
        final ColumnHolder colHolder = index.getColumnHolder(valueColumn);
        if (colHolder == null) {
          retVal = null;
          columns.put(valueColumn, retVal);
        } else {
          retVal = closer.register(colHolder.getColumn());
          columns.put(valueColumn, retVal);
        }
      }

      if (retVal instanceof DictionaryEncodedColumn) {
        //noinspection unchecked
        return (DictionaryEncodedColumn<String>) retVal;
      }

      throw new ISE("column [%s] is not dictionary encoded, cannot be used in lookup.", valueColumn);
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

    private ImmutableBitmap filterBitmap = null;

    private SegmentFilteredLookupExtractor(
        SegRefHolder segRef
    )
    {
      this.segRef = segRef;
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
}
