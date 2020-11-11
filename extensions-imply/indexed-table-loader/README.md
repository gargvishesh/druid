<!--
  ~ Copyright (c) 2019 Imply Data, Inc. All rights reserved.
  ~
  ~ This software is the confidential and proprietary information
  ~  of Imply Data, Inc.
  -->

# indexed-table-loader

A druid extension that loads an ingestion spec into a RowBasedIndexedTable for faster joins

To use this extension install it on the historicals and brokers. And start them with `druid.indexedTable.loader.configFilePath` set
to point to a json file that contains a map of indexed table name to `IndexedTableConfig`

If you are running realtime queries, it's also needed on MMs/Indexers.

WARNING: Large indexed tables seem problematic with the MM/peon model, so it's probably most suitable on indexers

For the full list of available configuration look at `io.imply.druid.indextable.loader.config.IndexedTableLoaderConfig`

The extension will load the data from the ingestion spec in a daemon thread on startup and keep it in memory.

## Version compatibility
This section aims to describe the version incompatible changes in the extension.
* 0.18.0 - First supported druid version
* 0.18.0 is only compatible with indexed-table-loader-0.18.0_1 
* 0.18.1 is compatible with indexed-table-loader-0.18.0_2 
* 0.19.0 is TBD

| Druid version \ Indexed table version  | indexed-table-loader-0.18.0_1  | indexed-table-loader-0.18.0_1  | master  |
|---|---|---|---|
| 0.18.0 | y | n | n |
| 0.18.1 | n | y | y |
| 0.19.0 | n | y | y |
