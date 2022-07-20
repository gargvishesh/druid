---
id: setup
sidebar_label: Setup
title: Enable the Multi-Stage Query (MSQ) Framework
---

> The Multi-Stage Query (MSQ) Framework is a preview feature available starting in Imply 2022.06. Preview features enable early adopters to benefit from new functionality while providing ongoing feedback to help shape and evolve the feature. All functionality documented on this page is subject to change or removal in future releases. Preview features are provided "as is" and are not subject to Imply SLAs.

## Prerequisites

To use the Multi-Stage Query (MSQ) Framework, make sure you meet the following requirements:

- An Imply Enterprise or Enterprise Hybrid cluster that runs version 2022.06 STS or later. Imply recommends using the latest STS version. MSQ isn't available in an LTS release yet. 
- Administrator access to Imply Manager so that you can enable MSQ. For security information related to datasources, see [Security](./msq-security.md).


## Enable MSQ in Imply

The setup process for Imply Hybrid (formerly Imply Cloud) is different from Imply Enterprise (formerly Imply Private). Imply Enterprise requires additional steps.

### Enable MSQ in Imply Hybrid

For Imply Hybrid, you enable MSQ by selecting the feature flag for it in Imply Manager: 

1. Go to **Clusters > Manage** for the cluster you want to enable the feature on. You need to enable MSQ on each individual cluster.
2. Go to **Setup** and expand the **Advanced config** options.
3. Edit the enabled **Feature flags** by clicking the pencil icon.
  - Select **Multi-Stage Query Engine (MSQE) - Beta**.
  - Clear **Use Indexers instead of Middle Managers**. This is not supported.
4. Apply the changes to your cluster. The cluster state changes to **UPDATING**, and MSQ will be available when the updates complete.

### Enable MSQ in Imply Enterprise

For Imply Enterprise, you need to load the extension to enable MSQ. In Imply Manager, perform the following steps:

1. Go to **Clusters > Manage** for the cluster you want to enable the feature on. You need to enable MSQ on each individual cluster.
2. Go to **Setup** and expand the **Advanced config** options.
3. Add the following custom extensions:

   **Required**
   - **Name**: `imply-talaria`
   - **S3 path or url**: Leave this blank. This extension is bundled with the Imply distribution.

   **Optional**
   The `druid-s3-extensions` is used for durable storage for shuffles, which can improve fault tolerance for queries that use more than 20 workers. Note that only S3 is supported for the storage type.
   - **Name**: `druid-s3-extensions`
   - **S3 path or url**: Leave this blank. This extension is bundled with the Imply distribution.

4. Edit the enabled **Feature flags** and clear **Use Indexers instead of Middle Managers**. This is not supported.

5. Add the following service properties:
   
   **Middle Manager** service properties configure how Middle Managers execute tasks. You can change the sample values provided to match your usage.

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

6. Apply the changes. The cluster restarts and MSQ will be available after the restart.

## Enable durable storage

Optionally, you can enable durable storage for mesh shuffles. Using this feature can improve the reliability of queries that use more than 20 workers. Note that only S3 is supported for the storage type. For more information about this feature, see [Durable storage for mesh shuffle](./msq-advanced-configs.md#durable-storage-for-mesh-shuffle).

To enable the feature with Imply Manager:

1. Go to **Clusters > Manage** for the cluster you want to enable the feature on. You need to enable this on each individual cluster.
2. Go to **Setup** and expand the **Advanced config** options.
3. If you use Imply Hybrid, skip this step. If you use Imply Enterprise, load the `druid-s3-extensions` extension:
   - **Name**: `druid-s3-extensions` 
   - **S3 path or url**: Leave this blank. This extension is bundled with the Imply distribution.
4. Add the following **Common** service properties: 
     
   ```bash
   # Required for using durable storage for mesh shuffle
   druid.msq.intermediate.storage.enable=true
   druid.msq.intermediate.storage.type=s3
   druid.msq.intermediate.storage.bucket=<your_bucket>
   druid.msq.intermediate.storage.prefix=<your_prefix<>
   druid.msq.intermediate.storage.tempDir=</path/to/your/temp/dir>
   # Optional for using durable storage for mesh shuffle
   druid.msq.intermediate.storage.maxResultsSize=5GiB
   ```
5. In S3, verify that you have the correct permissions set:
   
- `s3:GetObject`
- `s3:PutObject`
- `s3:AbortMultipartUpload`
- `s3:DeleteObject`

   For information about what the permissions are used for, see [S3](./msq-security.md#s3).

6. Apply the changes to your cluster.

## Next steps

Now that you've set up MSQ, you can do the following tutorials:

- [Connect external data](./msq-tutorial-connect-external-data.md)
- [Convert a JSON ingestion spec](./msq-tutorial-convert-ingest-spec.md)
