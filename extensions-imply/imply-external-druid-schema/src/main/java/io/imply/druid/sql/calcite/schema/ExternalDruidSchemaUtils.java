/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.sql.calcite.schema;

import com.fasterxml.jackson.core.type.TypeReference;
import io.imply.druid.sql.calcite.schema.tables.entity.TableColumn;
import io.imply.druid.sql.calcite.schema.tables.entity.TableSchema;
import io.imply.druid.sql.calcite.schema.tables.entity.TableSchemaMode;
import org.apache.druid.java.util.emitter.EmittingLogger;
import org.apache.druid.query.TableDataSource;
import org.apache.druid.segment.column.ColumnHolder;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.sql.calcite.schema.SegmentMetadataCache;
import org.apache.druid.sql.calcite.table.DatasourceTable;
import org.apache.druid.sql.calcite.table.DatasourceTable.PhysicalDatasourceMetadata;
import org.apache.druid.sql.calcite.table.DruidTable;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class ExternalDruidSchemaUtils
{
  private static final EmittingLogger LOG = new EmittingLogger(ExternalDruidSchemaUtils.class);

  public static final TypeReference<Map<String, TableSchema>> TABLE_SCHEMA_MAP_TYPE_REFERENCE =
      new TypeReference<Map<String, TableSchema>>()
      {
      };

  public static ConcurrentMap<String, DruidTable> convertTableSchemasToDruidTables(
      Map<String, TableSchema> tableSchemaMap,
      SegmentMetadataCache segmentMetadataCache
  )
  {
    if (tableSchemaMap == null) {
      return new ConcurrentHashMap<>();
    }
    final ConcurrentMap<String, DruidTable> druidTableMap = new ConcurrentHashMap<>();
    for (Map.Entry<String, TableSchema> entry : tableSchemaMap.entrySet()) {
      final TableSchema tableSchema = entry.getValue();
      if (tableSchema == null) {
        LOG.warn("Got a null table schema for table name: " + entry.getKey());
        continue;
      }

      RowSignature.Builder rowSignatureBuilder = RowSignature.builder();
      // columns from external source
      final Set<String> externalColumnNames = new HashSet<>();
      for (TableColumn tableColumn : tableSchema.getColumns()) {
        rowSignatureBuilder.add(tableColumn.getName(), tableColumn.getType());
        externalColumnNames.add(tableColumn.getName());
      }

      if (tableSchema.getSchemaMode().equals(TableSchemaMode.FLEXIBLE)) {
        // get the columns from segment metadata cache
        DatasourceTable.PhysicalDatasourceMetadata dsMetadata =
            segmentMetadataCache.getDatasource(tableSchema.getName());
        if (dsMetadata == null) {
          // add the __time column which will always be present, so this appears in the web console
          if (!externalColumnNames.contains(ColumnHolder.TIME_COLUMN_NAME)) {
            rowSignatureBuilder.addTimeColumn();
          }
        } else {
          // combine columns from external table and segment metadata cache, prioritizing those from external table
          for (String colName : dsMetadata.rowSignature().getColumnNames()) {
            // default to string but should never happen if the name is present in the row signature
            if (!externalColumnNames.contains(colName)) {
              ColumnType colType = dsMetadata.rowSignature().getColumnType(colName).orElse(ColumnType.STRING);
              rowSignatureBuilder.add(colName, colType);
            }
          }
        }
      }

      druidTableMap.put(
          tableSchema.getName(),
          new DatasourceTable(
              new PhysicalDatasourceMetadata(
                  new TableDataSource(tableSchema.getName()),
                  rowSignatureBuilder.build(),
                  false,
                  false
              )
          )
      );
    }
    return druidTableMap;
  }
}
