---
id: index
title: Overview
---

> The Multi-Stage Query framework is a preview feature available starting in Imply 2022.06. Preview features enable early adopters to benefit from new functionality while providing ongoing feedback to help shape and evolve the feature. All functionality documented on this page is subject to change or removal in future releases. Preview features are provided "as is" and are not subject to Imply SLAs.

The Multi-Stage Query (MSQ) framework provides a multi-stage distributed query engine called the SQL task engine that extends Druid's query capabilities. You can query [external data](./msq-queries.md#read-external-data) as well as perform ingestion with SQL [INSERT](./msq-queries.md#insert-data) and [REPLACE](./msq-queries.md#replace-data) queries. These queries, referred to as task queries, use the SQL task engine that's part of the MSQ framework.

MSQ excels at executing queries that can get bottlenecked at the Broker when using Druid's core query engine. When you submit task queries, MSQ splits them into stages and automatically exchanges data between stages. Each stage is parallelized to run across multiple data servers at once, simplifying performance.

In its current state, MSQ focuses on ingestion using INSERT and REPLACE to enable you to do the following with Druid:

- Read external data at query time using EXTERN.
- Execute batch ingestion jobs by writing SQL queries using INSERT and REPLACE. You no longer need to generate a JSON-based ingestion spec.
- Transform and rewrite existing tables using SQL.
- Perform multi-dimension range partitioning reliably, leading to segment sizes being distributed more evenly and better performance.

In addition, MSQ can do the following as part of a proof of concept or demo: 

- Execute heavy-weight queries that might run for a long time and return large numbers of rows.
- Execute queries that exchange large amounts of data between servers, like exact count distinct of high-cardinality fields.

Do not use or rely upon these queries for any meaningful use cases, especially production ones.

You can read more about the motivation for building MSQE in this [Imply blog post](https://imply.io/blog/a-new-shape-for-apache-druid/).

## Next steps

* [Understand how MSQ works](./msq-concepts.md) by reading more about the concepts behind it and its processes.
* [Explore the Query view](./msq-query-ui.md) to learn about the tools built into the UI that can help you get started.
* [Enable MSQ](./msq-setup.md) for your instance if you want to start running sample queries.

