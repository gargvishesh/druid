/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.sql.calcite.view.state.manager.sql;

import com.google.common.collect.ImmutableList;
import io.imply.druid.sql.calcite.view.ImplyViewDefinition;
import io.imply.druid.sql.calcite.view.state.manager.ViewAlreadyExistsException;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.metadata.SQLMetadataConnector;
import org.skife.jdbi.v2.Query;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

public class ViewStateSqlMetadataConnector
{
  private final SQLMetadataConnector connector;
  private final ViewStateSqlMetadataConnectorTableConfig tablesConfig;

  public ViewStateSqlMetadataConnector(
      SQLMetadataConnector connector,
      ViewStateSqlMetadataConnectorTableConfig tablesConfig
  )
  {
    this.connector = connector;
    this.tablesConfig = tablesConfig;
  }

  public void init()
  {
    if (tablesConfig.isCreateTables()) {
      createViewTable();
    }
  }

  public void createViewTable()
  {
    final String tableName = tablesConfig.getViewsTable();
    connector.createTable(
        tableName,
        ImmutableList.of(
            StringUtils.format(
                "CREATE TABLE %1$s (\n"
                + "  view_name VARCHAR(255) NOT NULL,\n"
                + "  view_namespace VARCHAR(255) NOT NULL,\n"
                + "  view_sql %2$s NOT NULL,\n"
                + "  modified_timestamp BIGINT NOT NULL,\n"
                + "  PRIMARY KEY (view_name, view_namespace)\n"
                + ")",
                tableName,
                connector.getPayloadType()
            )
        )
    );
  }

  public List<ImplyViewDefinition> getViews()
  {
    return connector.getDBI().inTransaction(
        (handle, status) -> {
          final Query<Map<String, Object>> query;

          String baseQuery = StringUtils.format(
              "SELECT"
              + " view_name,"
              + " view_namespace,"
              + " view_sql,"
              + " modified_timestamp"
              + " FROM %1$s",
              tablesConfig.getViewsTable()
          );

          query = handle.createQuery(baseQuery);

          return query.map((index, r, ctx) -> {
            final String viewName = r.getString("view_name");
            final String viewNamespace = r.getString("view_namespace");
            final long modifiedTimestamp = r.getLong("modified_timestamp");
            byte[] viewSqlBytes = r.getBytes("view_sql");

            return new ImplyViewDefinition(
                viewName,
                ImplyViewDefinition.DEFAULT_NAMESPACE.equals(viewNamespace) ? null : viewNamespace,
                StringUtils.fromUtf8(viewSqlBytes),
                DateTimes.utc(modifiedTimestamp)
            );
          }).list();
        });
  }

  public int createView(ImplyViewDefinition view)
  {
    AtomicBoolean alreadyExists = new AtomicBoolean();
    int created = connector.getDBI().inTransaction(
        (handle, transactionStatus) -> {

          String select = StringUtils.format(
              "SELECT COUNT(1) as count FROM %1$s WHERE view_name=:vn AND view_namespace=:vns",
              tablesConfig.getViewsTable()
          );

          int existingCount = handle.createQuery(select)
                                    .bind("vn", view.getViewName())
                                    .bind("vns", view.getViewNamespaceOrDefault())
                                    .map((index, r, ctx) -> r.getInt("count"))
                                    .first();
          if (existingCount > 0) {
            alreadyExists.set(true);
            return 0;
          }

          final String query = StringUtils.format(
              "INSERT INTO %1$s (view_name, view_namespace, view_sql, modified_timestamp) VALUES (:vn, :vns, :vs, :ts)",
              tablesConfig.getViewsTable()
          );
          return handle.createStatement(query)
                       .bind("vn", view.getViewName())
                       .bind("vns", view.getViewNamespaceOrDefault())
                       .bind("vs", StringUtils.toUtf8(view.getViewSql()))
                       .bind("ts", view.getLastModified().getMillis())
                       .execute();
        }
    );

    if (alreadyExists.get()) {
      if (view.getViewNamespace() != null) {
        throw new ViewAlreadyExistsException(view.getViewName(), view.getViewNamespace());
      } else {
        throw new ViewAlreadyExistsException(view.getViewName());
      }
    }
    return created;
  }

  public int alterView(ImplyViewDefinition view)
  {
    return connector.getDBI().inTransaction(
        (handle, transactionStatus) -> {
          final String updateQuery;
          updateQuery = StringUtils.format(
              "UPDATE %1$s SET modified_timestamp=:ts, view_sql=:vs WHERE view_name=:vn AND view_namespace=:vns",
              tablesConfig.getViewsTable()
          );
          return handle.createStatement(updateQuery)
                       .bind("vn", view.getViewName())
                       .bind("vns", view.getViewNamespaceOrDefault())
                       .bind("vs", StringUtils.toUtf8(view.getViewSql()))
                       .bind("ts", view.getLastModified().getMillis())
                       .execute();
        }
    );
  }

  public int deleteView(ImplyViewDefinition view)
  {
    return deleteView(view.getViewName(), view.getViewNamespaceOrDefault());
  }

  public int deleteView(String viewName, String viewNamespace)
  {
    return connector.getDBI().inTransaction(
        (handle, transactionStatus) -> {
          final String deleteStatement;
          deleteStatement = StringUtils.format(
              "DELETE from %1$s WHERE view_name=:vn AND view_namespace=:vns",
              tablesConfig.getViewsTable()
          );
          return handle.createStatement(deleteStatement)
                .bind("vn", viewName)
                .bind("vns", viewNamespace == null ? ImplyViewDefinition.DEFAULT_NAMESPACE : viewNamespace)
                .execute();
        }
    );
  }
}
