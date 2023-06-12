<!--
  ~ Copyright (c) Imply Data, Inc. All rights reserved.
  ~
  ~ This software is the confidential and proprietary information
  ~  of Imply Data, Inc.
  -->

# imply-lookups

A druid extension that enables running lookups on top of broadcast segments.

At its simplest, the implementation depends on the bitmap indexes for filtering values and finding
a matching row.  There is room for more optimal implementations (like the indexed table) or other ways of
optimizing what is used for the lookup that can be explored in the future.

# Usage

This extension should be added to all node types.  Once added, it will be possible to define a lookup like so

```json
{
  "type": "implySegment",
  "table": "lookup-data",
  "filterColumns": [
    "colA"
  ]
}
```

This defines a lookup over table "lookup-data" and also enables the usage of extra columns for filters.
Given that filterColumns is an array, it's possible to define multiple, but the example above defines only one: "colA"

Once that lookup is defined and has been pushed out across the cluster, it can be accessed via SQL like

```
lookup(columnX, 'lookup_table_name[key_to_lookup][column_to_read_for_value][colA_filter_value]') AS lookedid_up
```

Or the equivalent native lookup would be 

```json
{
    "type": "lookup",
    "dimension": "columnX",
    "outputName": "lookedid_up",
    "retainMissingValue": false,
    "replaceMissingValueWith": null,
    "name": "lookup_table_name[key_to_lookup][column_to_read_for_value][colA_filter_value]",
    "optimize": true
  }
```

# Known Caveats to Usage

1. Tables used for lookups must be single-segment tables.
2. At the time of writing/merge, there is a race in loading of the broadcast segments which means that it is not 
   possible to ensure that tasks receive a copy of their broadcast segment(s) before ingesting data.  This means
   that this lookup is primarily only useful for query-only use cases.  This caveat can be relaxed in the future
   when we add the ability for nodes to check in with the coordinator at startup to load up their broadcast
   segments.