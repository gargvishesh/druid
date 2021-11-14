<!--
  ~ Copyright (c) Imply Data, Inc. All rights reserved.
  ~
  ~ This software is the confidential and proprietary information
  ~ of Imply Data, Inc. You shall not disclose such Confidential
  ~ Information and shall use it only in accordance with the terms
  ~ of the license agreement you entered into with Imply.
  -->

# ts-ext
(Design by Eric Tschetter (@cheddar))

This is the "timeseries" (ts) extension.  It adds a new type of data to Druid called a timeseries.  This might sound weird at first because Druid does timeseries, it has a timeseries query and all of that stuff.  While that is true, Druid's actual storage and management of timeseries is closer to a relational database than it is to a traditional tsdb, specifically, internally it is storing a single data point included in a tuple with the dimensions.  This means that when doing timeseries analysis, the underlying storage doesn't actually have all of the points of the timeseries immediately available to do the analysis with.  It also means that Druid misses out on some opportunities for compression/optimization of storage of timeseries data, especially when trying to store raw data.  This extension changes the calculus a bit for Druid when it comes to timeseries data.  Specifically this aims to

1. Support expanded time ranges, i.e. allow for nanoseconds (or smaller?)  -- (aspirational, not in v1)
2. Co-locate timeseries data to allow for better compression techniques and enable things like interpolating values and loading extra time periods to fill in boundary values

The extension is primarily a new aggregator and resulting complex column.  There are some nuances around the organization of timeseries data that could allow for the creation of a new query in the future that takes advantage of the locality of the timeseries data structure, but that is something left as a future development assuming that this initial implementation adds value.

## Aggregator

The aggregator is the `imply-ts` aggregator, it can read from

* a numerical column, ***OR***
* a ts column created by using the aggregator during ingestion.

It's schema is as follows

```
{
  "type": "imply-ts",
  "tsColumn": "name_of_ts_column", <-- Optional, one of tsColumn or numericalColumn must be present
  "numericalColumn": "name_of_numerical_column", <-- Optional, one of tsColumn or numericalColumn must be present
  "downsample": {
    "period": "period_to_sample", 
    "fn": "function_to_use_to_downsample"
  },
  "interpolation": "type_of_interpolation", <-- Optional
  "window": "some_interval", <-- Optional
}
```

### Downsampling

`downsample` is a complex object used to define how to downsample a timeseries.  It has two parameters, a `period` 
which defines the time buckets to downsample into, and a `fn` which defines the function to use to downsample when multiple data points exist inside of the same period.  Supported `fn` values are ...  (maybe mimic the metric types from tsdbs?).

Generally downsampling technique is contextual to the type of metric the timeseries is supporting - to make logical 
sense of the data points even after downsampling. A few implicit/explicit types of metrics that a timeseries can 
encapsulate are (adapted from Prometheus types) : 
1. Counter : A counter is a cumulative metric that represents a single monotonically increasing counter whose value can only increase or be reset to zero on restart. For example, you can use a counter to represent the number of requests served, tasks completed, or errors.
2. Gauge : A gauge is a metric that represents a single numerical value that can arbitrarily go up and down. Gauges are typically used for measured values like temperatures or current memory usage, but also "counts" that can go up and down, like the number of concurrent requests.
3. Histogram / Summary : They are generally used to derive quantile information for the metrics. For instance, 
   deriving the 95th percentile time of the request duration for a service. There are different data structures 
   which can help derive this info (probabilistic and deterministic) but currently we've not decided upon the one to 
   choose.
   
We'll first try to propose some downsampling methods for Counter and Gauage metric types. The common ones between 
them are MIN, MAX, AVG (SUM + COUNT). From the prometheus documents (https://prometheus.io/docs/prometheus/latest/querying/functions/#aggregation_over_time), another possible to try could be LAST, where 
the representative value is the last value present in that time interval.

Some common functions used on top of Counters and Guages are ```rate()``` and ```delta()```  respectively. Rate is 
the amount of change in a counter per second and Delta is the difference between the starting data 
point of a time range. But I'm not sure whether they correspond to post-aggregators for us 
(since our aggregator is producing a time series and these functions can be applied on that aggregated structure)
or downsampling methods.
    
### Interpolation
Supported interpolation types are `"step"` and `"linear"`

### Visible Window
`window` defines a window to actually show values from.  This is useful when you want to try to ensure that you have values for the start and end of the timeseries window.  If window is defined, then the final result `ts` and `values` arrays will only include values with timestamps inside of the visible window, but there will also be an extra `bounds` field included in the result which is the nearest data point for the timeseries *not* inside of the visible window on both the start and end of the timeseries.  The range of time that `bounds` is taken from is limited by the interval of the query.

### Result Format

Resulting values will be of the form

```
{
  "ts": [64-bit_long_timestamp_of_granularity, ...],
  "values": [value, ...],
  "bounds": {
    "start": { "ts": "a_ts", "value": "a_value" },
    "end": { "ts": "a_ts", "value": "a_value" }
  }
}
```

Where the arrays `ts` and `values` are equal size.