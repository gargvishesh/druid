package org.hackathon;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;
import java.util.Set;

import static java.lang.Thread.sleep;

/**
 * Hello world!
 *
 */
public class App
{
    public static void main(String[] args) throws InterruptedException
    {


        AWSClient awsClient = new AWSClient("vishesh-imply-test", 10);
        DruidClient druidClient = new DruidClient(
            "jdbc:avatica:remote:url=http://localhost:8888/druid/v2/sql/avatica/;transparent_reconnection=true");
        while (true) {
            Set<String> currentObjects = awsClient.getObjects("topics/spooldir-json-topic");
            druidClient.sendInsertFromS3Query(currentObjects);
            druidClient.sendCompactionQuery();
            awsClient.deleteObjects(currentObjects);
            sleep(1000);
        }
    }
}
