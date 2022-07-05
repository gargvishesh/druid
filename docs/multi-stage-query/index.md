---
id: index
title: Overview
---

> The Multi-Stage Query Engine is a preview feature available starting in Imply 2022.06. Preview features enable early adopters to benefit from new functionality while providing ongoing feedback to help shape and evolve the feature. All functionality documented on this page is subject to change or removal in future releases. Preview features are provided "as is" and are not subject to Imply SLAs.

The Multi-Stage Query Engine (MSQE) is a multi-stage distributed query engine for Apache Druid that extends Druid's query capabilities. Use MSQE to query [external data](./msqe-sql-syntax.md#extern) as well as perform ingestion with SQL [INSERT](./msqe-sql-syntax.md#insert) and [REPLACE](./msqe-sql-syntax.md#replace) queries. MSQE excels at executing queries that can get bottlenecked at the Broker when using Druid's core query engine. When you run a query using MSQE, MSQE splits the query into stages and automatically exchanges data between stages. Each stage is parallelized to run across multiple data servers at once, simplifying performance.

In its current state, MSQE focuses on ingestion using INSERT and REPLACE to enable you to do the following with Druid:

- Read external data at query time using EXTERN.
- Execute batch ingestion jobs as SQL queries using INSERT and REPLACE. You no longer need to generate a JSON-based ingestion spec.
- Transform and rewrite existing tables using SQL queries.
- Perform multi-dimension range partitioning reliably, leading to segment sizes being distributed more evenly and better performance.

In addition, MSQE can do the following as part of a proof of concept or demo: 

- Execute heavy-weight queries that might run for a long time and return large numbers of rows.
- Execute queries that exchange large amounts of data between servers, like exact count distinct of high-cardinality fields.

Do not use or rely upon these queries for any meaningful use cases, especially production ones.

You can read more about the motivation for building MSQE in this [Imply blog post](https://imply.io/blog/a-new-shape-for-apache-druid/).

## Next steps

[Enable the Multi-Stage Query Engine (MSQE)](./msqe-setup.md) for your instance.

<!-- When the MSQE docs are unhidden for OSS, they'll be in the following order and location for the duration of the previews:

docs:

    "Getting started"
      ....

    "Tutorials"
      ...

    "Multi-stage query": [
      "multi-stage-query/index",
      "multi-stage-query/msqe-setup",
      "multi-stage-query/msqe-quickstart",
      "multi-stage-query/msqe-sql-syntax",
      "multi-stage-query/msqe-api",
      "multi-stage-query/msqe-advanced",  
      "multi-stage-query/msqe-reference",
      "multi-stage-query/msqe-release"
    ],

The URLs for these are

Overview page
http://localhost:3000/docs/multi-stage-query/

Enable the extension
http://localhost:3000/docs/multi-stage-query/msqe-setup.html

Quickstart for creating/running queries
http://localhost:3000/docs/multi-stage-query/msqe-quickstart.html

Security and performance
http://localhost:3000/docs/multi-stage-query/msqe-advanced.html

Error codes, report fields, etc
http://localhost:3000/docs/multi-stage-query/msqe-reference.html

Release notes and known limitations
http://localhost:3000/docs/multi-stage-query/msqe-release.html
--> 
