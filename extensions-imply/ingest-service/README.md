<!--
  ~ Copyright (c) Imply Data, Inc. All rights reserved.
  ~
  ~ This software is the confidential and proprietary information
  ~ of Imply Data, Inc. You shall not disclose such Confidential
  ~ Information and shall use it only in accordance with the terms
  ~ of the license agreement you entered into with Imply.
  -->

# SaaSy Ingest Service

This extension adds a new `ingest-service` which parasites its way onto a Druid cluster in order to manage the submission of batch ingest tasks to the Druid Overlord.

Input source extensions such as `druid-s3-extensions`, and input format extensions, such as `druid-parquet-extensions`,  should be loaded for this service to be able to generate tasks to submit to the Overlord.

To build this extension into a distribution, be sure to set `-Pimply-saas` maven profile when performing the distribution build.
 
## Configuration

### Tenant
```
imply.ingest.tenant.accountId=imply
imply.ingest.tenant.clusterId=imply-dev-test
```

### Metadata Store

#### SQL
Store metadata in Druid cluster metadata store using SQL metadata connector. Note that a metadata store extension must also be loaded, such as `mysql-metadata-storage`, etc
```
imply.ingest.metadata.type=sql
```

#### SQL Tables
Customize table names (not required, default is `ingest_{tableName}` such as `ingest_jobs`)
```
imply.ingest.metadata.tables.jobs=my_custom_jobs_table_name
```

### File Store
`imply.ingest.files.type`

#### S3
interact with s3 files, to submit Druid ingestion tasks with `S3InputSource`
```
imply.ingest.files.type=s3
imply.ingest.files.s3.bucket=imply-scratch
```

#### Local
For testing with local disk to submit tasks using `LocalInputSource`
```
imply.ingest.files.type=local
imply.ingest.files.local.baseDir=/Users/clint/workspace/data/imply/ingest/
```


## Example Intellij Run Configuration

```xml
<component name="ProjectRunConfigurationManager">
  <configuration default="false" name="IngestService" type="Application" factoryName="Application">
    <extension name="coverage" enabled="false" merge="false" sample_coverage="true" runner="idea" />
    <option name="MAIN_CLASS_NAME" value="org.apache.druid.cli.Main" />
    <option name="VM_PARAMETERS" value="-server -Ddruid.service=imply/ingest -Duser.timezone=UTC -Dfile.encoding=UTF-8 -Xmx4g -XX:+UseG1GC -XX:MaxGCPauseMillis=100 -XX:MaxDirectMemorySize=2G -Djava.util.logging.manager=org.apache.logging.log4j.jul.LogManager -Dorg.jboss.logging.provider=slf4j -Dlog4j.configurationFile=$PROJECT_DIR$/core/src/main/resources/log4j2.xml -Dlog4j.configurationFile=$PROJECT_DIR$/core/src/main/resources/log4j2.xml -Ddruid.host=localhost -Ddruid.extensions.directory=$PROJECT_DIR$/distribution/target/extensions/ -Ddruid.extensions.hadoopDependenciesDir=$PROJECT_DIR$/distribution/target/hadoop-dependencies/ -Ddruid.extensions.loadList=[\&quot;ingest-service\&quot;,\&quot;mysql-metadata-storage\&quot;,\&quot;druid-s3-extensions\&quot;,\&quot;druid-avro-extensions\&quot;,\&quot;druid-parquet-extensions\&quot;,\&quot;druid-orc-extensions\&quot;] -Ddruid.zk.service.host=localhost -Ddruid.metadata.storage.type=mysql -Ddruid.metadata.storage.connector.connectURI=&quot;jdbc:mysql://localhost:3306/druid&quot; -Ddruid.metadata.storage.connector.user=druid -Ddruid.metadata.storage.connector.password=diurd -Ddruid.emitter=logging -Ddruid.server.http.numThreads=100 -Ddruid.emitter.logging.logLevel=debug -Ddruid.generic.useDefaultValueForNull=false -Dimply.ingest.tenant.accountId=imply -Dimply.ingest.tenant.clusterId=clint-laptop -Dimply.ingest.files.type=local -Dimply.ingest.files.local.baseDir=/Users/clint/workspace/data/imply/ingest/ -Dimply.ingest.metadata.type=sql" />
    <option name="PROGRAM_PARAMETERS" value="server ingest-service" />
    <option name="WORKING_DIRECTORY" value="file://$PROJECT_DIR$" />
    <option name="ALTERNATIVE_JRE_PATH_ENABLED" value="false" />
    <option name="ALTERNATIVE_JRE_PATH" value="1.8" />
    <option name="ENABLE_SWING_INSPECTOR" value="false" />
    <option name="ENV_VARIABLES" />
    <option name="PASS_PARENT_ENVS" value="true" />
    <module name="druid-services" />
    <envs />
    <method />
  </configuration>
</component>
```