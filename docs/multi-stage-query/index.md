---
id: index
title: Overview
---

> The multi-stage query architecture and its SQL-task engine are experimental features available starting in Druid 24.0. You can use it in place of the existing native batch and Hadoop based ingestion systems. As an experimental feature, functionality documented on this page is subject to change or removal in future releases. Review the release notes and this page to stay up to date on changes.

The multi-stage query architecture for Apache Druid includes a multi-stage distributed query engine called the SQL-task engine that extends Druid's query capabilities. With the SQL-task engine, you can write task queries that can reference [external data](./msq-queries.md#read-external-data) as well as perform ingestion with SQL [INSERT](./msq-queries.md#insert-data) and [REPLACE](./msq-queries.md#replace-data). Essentially, you can perform SQL-based ingestion instead of using JSON ingestion specs that Druid's native ingestion uses.

The multi-stage query architecture and SQL-task engine excel at executing queries that can get bottlenecked at the Broker when using Druid's native SQL engine. When you submit task queries, the SQL-task engine splits them into stages and automatically exchanges data between stages. Each stage is parallelized to run across multiple data servers at once, simplifying performance.

In its current state, the SQL-task engine enables you to do the following:

- Read external data at query time using EXTERN.
- Execute batch ingestion jobs by writing SQL queries using INSERT and REPLACE. You no longer need to generate a JSON-based ingestion spec.
- Transform and rewrite existing tables using SQL.
- Perform multi-dimension range partitioning reliably, which leads to more evenly distributed segment sizes and better performance.

The SQL-task engine has additional features that can be used as part of a proof of concept or demo, but don't use or rely on the following features for any meaningful use cases, especially production ones:

- Execute heavy-weight queries and return large numbers of rows.
- Execute queries that exchange large amounts of data between servers, like exact count distinct of high-cardinality fields.


## Load the extension

For new clusters that use 24.0 or later, the multi-stage query extension is loaded by default. If you want to add the extension to an existing cluster, add the extension `druid-multi-stage-query` to `druid.extensions.loadlist` in your `common.runtime.properties` file.

For more information about how to load an extension, see [Loading extensions](../development/extensions.md#loading-extensions).

Note that to use EXTERN, users need READ permission on the resource named "EXTERNAL" of the resource type "EXTERNAL". If you encounter a 403 error when trying to use EXTERN, verify that you have the correct permissions.

## Next steps

* [Understand how the multi-stage query architecture works](./msq-concepts.md) by reading about the concepts behind it and its processes.
* [Explore the Query view](./msq-query-ui.md) to learn about the UI tools that can help you get started.

