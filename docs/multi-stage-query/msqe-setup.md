---
id: msqe-setup
sidebar_label: Setup
title: Enable the multi-stage query engine
---

> The multi-stage query engine is a preview feature available starting in Imply 2022.06. Preview features enable early adopters to benefit from new functionality while providing ongoing feedback to help shape and evolve the feature. All functionality documented on this page is subject to change or removal in future releases. Preview features are provided "as is" and are not subject to Imply SLAs.

## Prerequisites

To use the Multi-Stage Query Engine (MSQE), make sure you meet the following requirements:

- An Imply Enterprise or Enterprise Hybrid cluster that runs version 2022.06 STS or later. Imply recommends using the latest STS version. MSQE isn't available in an LTS release yet. 
- Administrator access to Imply Manager so that you can enable MSQE. For security information related to datasources, see [Security](./msqe-advanced-configs.md#security).


## Enable MSQE in Imply

The setup process for Imply Hybrid (formerly Imply Cloud) is different from Imply Enterprise (formerly Imply Private). Imply Enterprise requires additional steps.

### Enable MSQE in Imply Hybrid

For Imply Hybrid, you enable MSQE by selecting the feature flag for it in Imply Manager: 

1. Go to **Clusters > Manage** for the cluster you want to enable the feature on. You need to enable MSQE on each individual cluster.
2. Go to **Setup** and expand the **Advanced config** options.
3. Edit the enabled **Feature flags** by clicking the pencil icon.
4. Select **Multi-Stage Query Engine (MSQE) - Beta** from the list and click **OK**.
5. Optionally, you can load the `druid-s3-extensions` that MSQE uses for durable storage based shuffles. Using this feature can improve the reliability of queries that use more than 20 workers. Note that only S3 is supported for the storage type.
   1. Add the following custom extension:
      - **Name**: `druid-s3-extensions` 
      - **S3 path or url**: Leave this blank. This extension is bundled with the Imply distribution.
   2. Add the following **Common** service properties: 
     
        ```bash
        # Required for using durable storage for mesh shuffle
        druid.talaria.intermediate.storage.enable=true
        druid.talaria.intermediate.storage=s3
        druid.talaria.intermediate.storage.bucket=<your_bucket>
        druid.talaria.intermediate.storage.prefix=<your_prefix<>
        druid.talaria.intermediate.storage.tempDir=</path/to/your/temp/dir>
        # Optional for using durable storage for mesh shuffle
        druid.talaria.intermediate.storage.maxResultsSize=5GiB
        ```
   
      For more information about these settings, see [Durable storage for mesh shuffle](./msqe-advanced-configs.md#durable-storage-for-mesh-shuffle). Additionally, certain permissions are required for [S3](./msqe-advanced-configs.md#s3).

6. Apply the changes to your cluster. The cluster state changes to **UPDATING**, and MSQE will be available when the updates complete.

### Enable MSQE in Imply Enterprise

For Imply Enterprise, you need to load the extension to enable MSQE. In Imply Manager, perform the following steps:

1. Go to **Clusters > Manage** for the cluster you want to enable the feature on. You need to enable MSQE on each individual cluster.
2. Go to **Setup** and expand the **Advanced config** options.
3. Add the following custom extensions:

   **Required**
   - **Name**: `imply-talaria`
   - **S3 path or url**: Leave this blank. This extension is bundled with the Imply distribution.

   **Optional**
   The `druid-s3-extensions` is used for durable storage for shuffles, which can improve fault tolerance for queries that use more than 20 workers. Note that only S3 is supported for the storage type.
   - **Name**: `druid-s3-extensions`
   - **S3 path or url**: Leave this blank. This extension is bundled with the Imply distribution.

4. Change the following feature flags by clicking the pencil icon:

   - Select **HTTP-based task management**. Turning feature is not required but can improve performance.
   - Clear **Use Indexers instead of Middle Managers**. Turning off this feature is not required but can improve performance.

5. Add the following **Service properties**:

   **Common** service properties are configurations that are shared across all Druid services. The following properties are used if you want to enable durable storage for shuffles with MSQE queries. If you are not using it, skip to the **Middle Manager** properties.

   ```bash
   # Required for using durable storage for mesh shuffle
   druid.talaria.intermediate.storage.enable=true
   druid.talaria.intermediate.storage=s3
   druid.talaria.intermediate.storage.bucket=<your_bucket>
   druid.talaria.intermediate.storage.prefix=<your_prefix<>
   druid.talaria.intermediate.storage.tempDir=</path/to/your/temp/dir>
   # Optional for using durable storage for mesh shuffle
   druid.talaria.intermediate.storage.maxResultsSize=5GiB
   ```
   
   For more information about these settings, see [Durable storage for mesh shuffle](./msqe-advanced-configs.md#durable-storage-for-mesh-shuffle). Additionally, certain permissions are required for [S3](./msqe-advanced-configs.md#s3).

   **Middle Manager** service properties configure how Middle Managers execute tasks. You can change the sample values provided in this quickstart to match your usage.

   ```bash
   # Set this property to the maximum number of tasks per job plus 25.
   # The upper limit for tasks per job is 1000, so 1000 + 25.
   # Set this lower if you do not intend to use this many tasks.
   druid.indexer.fork.property.druid.server.http.numThreads=1025
   
   # Lazy initialization of the connection pool that used for shuffle.
   druid.global.http.numConnections=50
   druid.global.http.eagerInitialization=false
   
   # Required for Java 11
   jvm.config.dsmmap=--add-exports=java.base/jdk.internal.ref=ALL-UNNAMED
   druid.indexer.runner.javaOptsArray=["--add-exports=java.base/jdk.internal.ref=ALL-UNNAMED"]
   ```

   **Broker** service properties control how the Broker routes queries. Set your SQL executor to `imply`:

   ```bash
   druid.sql.executor.type=imply
   ```

6. Apply the changes to your deployment. The deployment restarts and MSQE will be available after the restart.

## Next steps

Start with the [Multi-stage query engine quickstart](./msqe-quickstart.md). It walks you through running queries against external datasources that are publicly hosted.
