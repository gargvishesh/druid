<!--
  ~ Copyright (c) Imply Data, Inc. All rights reserved.
  ~
  ~ This software is the confidential and proprietary information
  ~  of Imply Data, Inc.
  -->

This folder contians examples of indexed table loader specs that shows how to
configure the extension to read an indexed table. These specs are useful up to
v 0.18.1 of Apache Druid. Starting with v 0.19, this extension will load
broadcasted segments instead of reading a config file. At such time, these
examples can be removed or re-purposed.

### windex

The `windex` index table is meant to parallel the `lookyloo` lookup used in
CalciteQueryTests. You can think of `windex` as a fat version of `lookyloo`.

It has the following keys:
- key: A string key that mimics the `k` key of lookyloo. This column contains
a null key, which a lookup can not support.
- stringDim: This column mimics the `v` column of lookyloo. As of Druid 0.18.1,
the lookyloo table does not have a row where the value in a lookup is null.
- longDim: A longKey that can be used to test filter pushdowns of numeric key
columns.
- floatDim: A float key that can be used to match against other float columns,
for example `druid.foo#m1`

### wikipedia

This indexed table spec allows you to ingest the provided sample wikipedia
dataset as an indexed table. It has 3 keys, all of which are string columns:
- countryIsoCode
- cityName
- regionIsoCode

This index table is useful if you want to write test joining the wikipedia
datasource against itself as an indexed table vs issuing an inline query using the subquery mechanism in the broker.
