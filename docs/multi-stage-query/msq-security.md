---
id: security
title: Security for the Multi-Stage Query (MSQ) Framework
sidebar_label: Security
---

> The Multi-Stage Query (MSQ) Framework is a preview feature available starting in Imply 2022.06. Preview features enable early adopters to benefit from new functionality while providing ongoing feedback to help shape and evolve the feature. All functionality documented on this page is subject to change or removal in future releases. Preview features are provided "as is" and are not subject to Imply SLAs.

All authenticated users can use the Multi-Stage Query (MSQ) framework through the UI and API if the extension is loaded. However, without additional permissions, users are not able to issue queries that read or write Druid datasources or external data. The permission you need depends on what you are trying to do with MSQ.

## General permissions

The permission required to submit a query depends on the type of query:

  - SELECT from a Druid datasource requires the READ DATASOURCE permission on that
  datasource.
  - INSERT or REPLACE into a Druid datasource requires the WRITE DATASOURCE permission on that
  datasource.
  - EXTERN references to external data require READ permission on the resource name "EXTERNAL" of the resource type "EXTERNAL".

MSQ tasks are Overlord tasks, so they follow the Overlord's (indexer) model. This means that users with access to the Overlord API can perform some actions even if they didn't submit the query. The actions include retrieving the status or canceling a query. For more information about the Overlord API and MSQ, see [API](./msq-reference.md#context-parameters).

To interact with a query through the Overlord API, you need the following permissions:

- INSERT or REPLACE queries: You must have READ DATASOURCE permission on the output datasource.
- SELECT queries: You must have read permissions on the `__query_select` datasource, which is a stub datasource that MSQ creates.


## S3

If you enable durable storage for mesh shuffle with Amazon S3 as the storage, you need the following S3 permissions:
- Permissions for pushing and fetching intermediate stage results to and from S3:
  - `s3:GetObject`
  - `s3:PutObject`
  - `s3:AbortMultipartUpload`
- Permissions for removing intermediate stage results:
  - `s3:DeleteObject`
