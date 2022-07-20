---
id: index
title: Overview
---

> The Multi-Stage Query (MSQ) Framework is a preview feature available starting in Imply 2022.06. Preview features enable early adopters to benefit from new functionality while providing ongoing feedback to help shape and evolve the feature. All functionality documented on this page is subject to change or removal in future releases. Preview features are provided "as is" and are not subject to Imply SLAs.

The Multi-Stage Query (MSQ) Framework for Apache Druid includes a multi-stage distributed query engine called the SQL-task engine that extends Druid's query capabilities. When you use the SQL-task engine, you can write queries, called task queries, that can reference [external data](./msq-queries.md#read-external-data) as well as perform ingestion with SQL [INSERT](./msq-queries.md#insert-data) and [REPLACE](./msq-queries.md#replace-data). 

MSQ excels at executing queries that can get bottlenecked at the Broker when using Druid's core query engine. When you submit task queries, MSQ splits them into stages and automatically exchanges data between stages. Each stage is parallelized to run across multiple data servers at once, simplifying performance.

In its current state, MSQ enables you to do the following:

- Read external data at query time using EXTERN.
- Execute batch ingestion jobs by writing SQL queries using INSERT and REPLACE. You no longer need to generate a JSON-based ingestion spec.
- Transform and rewrite existing tables using SQL.
- Perform multi-dimension range partitioning reliably, which leads to more evenly distributed segment sizes and better performance.

In addition, MSQ can do the following as part of a proof of concept or demo: 

- Execute heavy-weight queries and return large numbers of rows.
- Execute queries that exchange large amounts of data between servers, like exact count distinct of high-cardinality fields.

Do not use these queries in production or for any meaningful use cases.

You can read more about the motivation for building MSQ in this [Imply blog post](https://imply.io/blog/a-new-shape-for-apache-druid/).

## Next steps

* [Understand how MSQ works](./msq-concepts.md) by reading about the concepts behind it and its processes.
* [Explore the Query view](./msq-query-ui.md) to learn about the UI tools that can help you get started.
* [Enable MSQ](./msq-setup.md) for your instance to start running sample queries.

