---
id: advanced
title: Advanced configs
---

> The Multi-Stage Query (MSQ) framework is a preview feature available starting in Imply 2022.06. Preview features enable early adopters to benefit from new functionality while providing ongoing feedback to help shape and evolve the feature. All functionality documented on this page is subject to change or removal in future releases. Preview features are provided "as is" and are not subject to Imply SLAs.


## Durable storage for mesh shuffle

By default, the Multi-Stage Query (MSQ) framework uses the local storage of a node to store data from intermediate steps when executing a query. Although this method provides MSQ with better speed when executing a query, the data is lost if the node encounters an issue. When you enable durable storage, MSQ stores intermediate data in S3 instead. In essence, you trade some performance for better reliability. This is especially useful for long running queries.

To use durable storage for mesh shuffles: 

- [Enable durable storage for mesh shuffle](./msq-setup.md)
- Use the appropriate [security settings for S3](./msq-security.md#s3)
- Include the following context variable when you submit a query:

**UI**

   ```sql
   --:context msqDurableShuffleStorage: true
   ```

**API**

   ```json
   "context": {
       "msqDurableShuffleStorage": true
   }
   ```

The following table describes the properties used to configure durable storage for MSQ:

| Config  | Description  | Required | Default |
|---------|--------------------------|----------|---------|
| `druid.msq.intermediate.storage.enable`                      | Set to `true` to enable this feature.  | Yes      | None    |
| `druid.msq.intermediate.storage.type`                      | Must be set to `s3`.  | Yes      | None    |
| `druid.msq.intermediate.storage.bucket`                    | S3 bucket to store intermediate stage. results.  | Yes      | None  |
| `druid.msq.intermediate.storage.prefix`                    | S3 prefix to store intermediate stage results. Provide a unique value for the prefix. Clusters should not share the same prefix.  | Yes      | None |
| `druid.msq.intermediate.storage.tempDir`                   | Directory path on the local disk to store intermediate stage results. temporarily.  | Yes      | None |
| `druid.msq.intermediate.storage.maxResultsSize`            | Max size of each partition file per stage. It should be between 5MiB and 5TiB. Supports a human-readable format.  For eg if a stage has 50 partitions we can effectively use s3 up to 250TIB of stage output assuming each partition file <=5TiB. | No       | 100MiB  |
| `druid.msq.intermediate.storage.chunkSize`                 | Imply recommends using the default value for most cases. This property defines the size of each chunk to temporarily store in `druid.msq.intermediate.storage.tempDir`. Druid computes the chunk size automatically if this property is not set. The chunk size must be between 5MiB and 5GiB. | No       | None    |
| `druid.msq.intermediate.storage.maxTriesOnTransientErrors` | Imply recommends using the default value for most cases. This property defines the max number times to attempt S3 API calls to avoid failures due to transient errors.  | no       | 10      |