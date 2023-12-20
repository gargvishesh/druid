package org.hackathon;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

public class DruidClient
{
  final String url;
  final Properties connectionProperties;

  public DruidClient(String url)
  {
    this.url = url;
    this.connectionProperties = new Properties();
    Map<String, String> context = new HashMap<>();
//    context.put("executionMode", "async")
//    connectionProperties.setProperty("context", context);
    connectionProperties.setProperty("executionMode", "async");
  }

  void sendQuery(String query)
  {
    try (Connection connection = DriverManager.getConnection(url, connectionProperties)) {
      try (
          final Statement statement = connection.createStatement();
          final ResultSet rs = statement.executeQuery(query)
      ) {

      }
    }
    catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  void sendInsertFromS3Query(Set<String> objects)
  {
    String query = String.format("INSERT INTO orders WITH ext AS (SELECT *\n"
                                 + "FROM TABLE(\n"
                                 + "  EXTERN(\n"
                                 + "    '{\"type\":\"s3\", \"objectGlob\": \"**.json\",\"urls\":%s}}',\n"
                                 + "    '{\"type\":\"json\"}'\n"
                                 + "  ) \n"
                                 + ") EXTEND (\"order_number\" BIGINT, \"order_date\" VARCHAR, \"purchaser\" BIGINT, \"quantity\" BIGINT, \"product_id\" BIGINT, \"record_ts\" VARCHAR, \"ts_ms\" BIGINT)\n"
                                 + ")\n"
                                 + "SELECT\n"
                                 + "  TIME_PARSE(\"record_ts\") AS \"__time\",\n"
                                 + "  \"order_number\",\n"
                                 + "  \"order_date\",\n"
                                 + "  \"purchaser\",\n"
                                 + "  \"quantity\",\n"
                                 + "  \"product_id\",\n"
                                 + "  \"ts_ms\"\n"
                                 + "FROM ext\n"
                                 + "PARTITIONED BY DAY", objects.toString());

    sendQuery(query);

  }

  void sendCompactionQuery()
  {
    String compactionQuery = "REPLACE INTO \"orders\" OVERWRITE ALL\n"
                             + "SELECT\n"
                             + "  __time,\n"
                             + "  \"order_number\",\n"
                             + "  LATEST_BY(\"order_date\", MILLIS_TO_TIMESTAMP(ts_ms)) as order_date,\n"
                             + "  LATEST_BY(\"purchaser\", MILLIS_TO_TIMESTAMP(ts_ms)) as purchaser,\n"
                             + "  LATEST_BY(\"quantity\", MILLIS_TO_TIMESTAMP(ts_ms)) as quantity,\n"
                             + "  LATEST_BY(\"product_id\", MILLIS_TO_TIMESTAMP(ts_ms)) as product_id,\n"
                             + "  LATEST_BY(\"ts_ms\", MILLIS_TO_TIMESTAMP(ts_ms)) as ts_ms\n"
                             + "FROM orders\n"
                             + "GROUP BY __time, order_number\n"
                             + "PARTITIONED BY DAY\n";

    sendQuery(compactionQuery);

  }


}
