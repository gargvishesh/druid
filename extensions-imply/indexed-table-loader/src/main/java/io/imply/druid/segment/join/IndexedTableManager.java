/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc.
 */

package io.imply.druid.segment.join;

import com.google.common.collect.ImmutableMap;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.segment.Segment;
import org.apache.druid.segment.join.table.IndexedTable;
import org.apache.druid.timeline.SegmentId;
import org.apache.druid.utils.CollectionUtils;

import javax.inject.Inject;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.function.UnaryOperator;

/**
 * Not much more than a {@link ConcurrentHashMap}, this should really just be part of Druid's SegmentManager
 */
public class IndexedTableManager
{
  private static final Logger LOG = new Logger(IndexedTableManager.class);

  private final ConcurrentHashMap<String, CloseTableFirstReferenceCountingIndexedTable> indexedTables;
  private final ConcurrentHashMap<String, SegmentId> blacklist;

  @Inject
  public IndexedTableManager()
  {
    this.indexedTables = new ConcurrentHashMap<>();
    this.blacklist = new ConcurrentHashMap<>();
  }

  public boolean containsIndexedTable(String tableName)
  {
    return indexedTables.containsKey(tableName);
  }

  public CloseTableFirstReferenceCountingIndexedTable getIndexedTable(String tableName)
  {
    LOG.debug("Getting indexed table %s", tableName);
    return indexedTables.get(tableName);
  }

  public Map<String, CloseTableFirstReferenceCountingIndexedTable> getIndexedTables()
  {
    Map<String, CloseTableFirstReferenceCountingIndexedTable> copy = ImmutableMap.copyOf(indexedTables);
    return CollectionUtils.mapKeys(copy, Function.identity());
  }

  public void addIndexedTable(String tableName, Segment segment, UnaryOperator<IndexedTable> tableFunction)
  {
    LOG.info("Loading indexed table %s", tableName);
    indexedTables.compute(
        tableName,
        (key, existingTable) -> {
          SegmentId existingToCompare = existingTable == null ? blacklist.get(tableName) : existingTable.getSegmentId();
          if (existingToCompare != null) {
            if (!segment.getId().getInterval().contains(existingToCompare.getInterval())) {
              LOG.error(
                  "Indexed tables must be created from a datasource with a single broadcast segment."
                  + " Existing indexed table %s with version %s and interval %s is not contained within the"
                  + " segment %s interval %s. Multiple segment backed indexed tables are not yet supported,"
                  + " dropping indexed table to avoid data consistency issues.",
                  tableName,
                  existingToCompare.getVersion(),
                  existingToCompare.getInterval(),
                  segment.getId(),
                  segment.getDataInterval()
              );
              blacklist.put(tableName, existingToCompare);
              if (existingTable != null) {
                existingTable.close();
              }
              return null;
            }
            if (existingToCompare.getPartitionNum() != segment.getId().getPartitionNum()) {
              LOG.error(
                  "Indexed tables must be created from a datasource with a single broadcast segment."
                  + " Existing indexed table %s with version %s created from segmentId %s attempted to load from"
                  + " a different segment %s of the same version within the same interval %s. Partitions are not yet"
                  + " supported, dropping indexed table to avoid data consistency issues.",
                  tableName,
                  existingToCompare.getVersion(),
                  existingToCompare,
                  segment.getId(),
                  segment.getDataInterval()
              );
              blacklist.put(tableName, existingToCompare);
              if (existingTable != null) {
                existingTable.close();
              }
              return null;
            }
          }
          // if we got here, then make sure the table has been removed from the blacklist
          blacklist.remove(tableName);
          if (existingTable == null) {
            LOG.info("Adding new indexed table %s with version %s", tableName, segment.getId().getVersion());
            return new CloseTableFirstReferenceCountingIndexedTable(tableFunction.apply(null), segment.getId());
          }
          if (existingTable.isOlderThan(segment)) {
            LOG.info("Replacing indexed table %s with version %s", tableName, segment.getId().getVersion());
            existingTable.close();
            LOG.info("Closed existing indexed table %s", tableName);
            return new CloseTableFirstReferenceCountingIndexedTable(tableFunction.apply(null), segment.getId());
          }
          LOG.warn(
              "Existing indexed table %s with version %s is newer than %s, ignoring load for segment %s",
              tableName,
              existingTable.version(),
              segment.getId().getVersion(),
              segment.getId()
          );
          return existingTable;
        }
    );
  }

  public void dropIndexedTable(Segment segmentToDrop)
  {
    final String tableName = segmentToDrop.getId().getDataSource();
    final String version = segmentToDrop.getId().getVersion();
    LOG.info("Dropping indexed table %s with version %s", tableName, version);
    indexedTables.compute(
        tableName,
        (key, existingTable) -> {
          if (existingTable == null ||
              existingTable.isOlderThan(segmentToDrop) ||
              existingTable.isSameVersion(segmentToDrop)) {
            if (existingTable != null) {
              LOG.info("Closing indexed table %s version %s", tableName, version);
              existingTable.close();
            }
            LOG.info("Dropped indexed table %s with version %s", tableName, version);
            return null;
          }
          LOG.info(
              "Indexed table %s version %s is newer than requested drop version %s, skipping drop",
              tableName,
              existingTable.version(),
              version
          );
          return existingTable;
        }
    );
  }
}
