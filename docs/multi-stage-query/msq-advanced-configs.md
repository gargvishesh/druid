---
id: advanced
title: Advanced configs
---

> The Multi-Stage Query (MSQ) Framework is a preview feature available starting in Imply 2022.06. Preview features enable early adopters to benefit from new functionality while providing ongoing feedback to help shape and evolve the feature. All functionality documented on this page is subject to change or removal in future releases. Preview features are provided "as is" and are not subject to Imply SLAs.

## Durable storage for mesh shuffle

By default, the Multi-Stage Query (MSQ) Framework uses the local storage of a node to store data from intermediate steps when executing a query. Although this method provides MSQ with better speed when executing a query, the data is lost if the node encounters an issue. When you enable durable storage, MSQ stores intermediate data in Amazon S3 instead. In essence, you trade some performance for better reliability. This is especially useful for long-running queries.

To use durable storage for mesh shuffles: 

- [Enable durable storage for mesh shuffle](./msq-setup.md)
- Use the appropriate [security settings for S3](./msq-security.md#s3)
- Configure your query to use durable storage. 
  - For the API, include `"durableShuffleStorage": true` in the query context when you submit a query like so:

     ```json
     "context": {
         "durableShuffleStorage": true
     }
     ```

  - For the Druid console, toggle this setting in the **Engine** menu.

The following table describes the properties used to configure durable storage for MSQ:

| Config | Description | Required | Default |
|--------|-------------|----------|---------|
| `druid.msq.intermediate.storage.enable` | Set to `true` to enable this feature. | Yes | |
| `druid.msq.intermediate.storage.type` | Set the value to `s3`. | Yes | |
| `druid.msq.intermediate.storage.bucket` | S3 bucket to store intermediate stage results. | Yes | |
| `druid.msq.intermediate.storage.prefix` | S3 prefix to store intermediate stage results. Provide a unique value for the prefix. Don't share the same prefix between clusters. | Yes | |
| `druid.msq.intermediate.storage.tempDir`| Directory path on the local disk to temporarily store intermediate stage results. | Yes | |
| `druid.msq.intermediate.storage.maxResultsSize` | Max size of each partition file per stage. It should be between 5 MiB and 5 TiB. Supports a human-readable format. For example, if a stage has 50 partitions and each partition file is below or equal to 5 TiB, you can effectively use S3 up to 250 TiB of stage output. | No | 100 MiB |
| `druid.msq.intermediate.storage.chunkSize` | Defines the size of each chunk to temporarily store in `druid.msq.intermediate.storage.tempDir`. The chunk size must be between 5 MiB and 5 GiB. Druid computes the chunk size automatically if no value is provided.| No | |
| `druid.msq.intermediate.storage.maxTriesOnTransientErrors` | Defines the max number times to attempt S3 API calls to avoid failures due to transient errors. | No | 10 |