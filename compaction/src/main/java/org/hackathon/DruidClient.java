package org.hackathon;

import com.jsoniter.JsonIterator;
import com.jsoniter.any.Any;
import com.jsoniter.output.JsonStream;
import com.jsoniter.spi.JsonException;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import static java.lang.Thread.sleep;

public class DruidClient
{
  final String url;
//  final Properties connectionProperties;

  final OkHttpClient client;

  final String bucket;

  public DruidClient(String url, String bucket)
  {
/*
    this.connectionProperties = new Properties();
    Map<String, String> context = new HashMap<>();
//    context.put("executionMode", "async")
//    connectionProperties.setProperty("context", context);
    connectionProperties.setProperty("executionMode", "async");

*/


    this.url = url;
    client = new OkHttpClient();
    this.bucket = bucket;


  }

  String sendQuery(String query)
  {
    MediaType JSON = MediaType.get("application/json");
    Map<String, Integer> context = Map.of("maxNumTasks", 3);
    Map<String, Object> json = Map.of("query", query, "context", context);
    String jsonString = JsonStream.serialize(json);
    System.out.println(jsonString);

    RequestBody body = RequestBody.create(jsonString, JSON);
    Request request = new Request.Builder()
        .url(url)
        .post(body)
        .build();
    try (Response response = client.newCall(request).execute()) {
      Any respObject = JsonIterator.deserialize(response.body().string());
      String taskId = respObject.get("taskId").toString();
      String status = respObject.get("state").toString();
      Response statusResponse;
      while (!status.equals("SUCCESS") || !status.equals("FAILED")) {
        try{
          sleep(1000);
          Request newRequest = new Request.Builder().url(String.format(
              "http://localhost:8888/druid/indexer/v1/task/%s/status",
              taskId
          )).build();
          Response newResponse = client.newCall(newRequest).execute();
          Any newRespObject = JsonIterator.deserialize(newResponse.body().toString());
          status = newRespObject.get("status", "status").toString();
        } catch(Exception e) {

        }
      }
      return "Done";
    }
    catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  void sendInsertFromS3Query(Set<String> objects)
  {
    String objectsList = String.join("\",\"s3://" + bucket + "/", objects);
    objectsList = "[\"s3://" + bucket + "/" + objectsList + "\"]";
    System.out.println(objectsList);

    String query = String.format("INSERT INTO orders WITH ext AS (SELECT *\n"
                                 + "FROM TABLE(\n"
                                 + "  EXTERN(\n"
                                 + "    '{\"type\":\"s3\",\"objectGlob\": \"**.json\",\"prefixes\":%s}}',\n"
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
                                 + "PARTITIONED BY DAY", objectsList);

    System.out.print("Query");
    System.out.print(query);

    System.out.println(sendQuery(query));

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

    System.out.println(sendQuery(compactionQuery));
  }


}
