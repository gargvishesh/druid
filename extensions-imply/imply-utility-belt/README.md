<!--
  ~ Copyright (c) Imply Data, Inc. All rights reserved.
  ~
  ~ This software is the confidential and proprietary information
  ~  of Imply Data, Inc.
  -->

<img src="https://static.imply.io/img/bat-belt.jpg" />

## CloudWatch logs parser

Parses the CloudWatch log container format. Inside the log container will be a list of other messages, which can
be parsed with another parser.

The CloudWatch parser will add three bonus fields to your messages:

- owner
- logGroup
- logStream

See https://docs.aws.amazon.com/AmazonCloudWatch/latest/logs/CloudWatchLogsConcepts.html for details on the meanings
of these fields. If the underlying events already have fields with these names, they will be overwritten.

An example for VPC Flow Logs:

```json
"parser": {
  "type": "cloudwatch",
  "parseSpec": {
    "format": "tsv",
    "delimiter": " ",
    "timestampSpec": {
      "column": "start",
      "format": "posix"
    },
    "columns": [
      "version",
      "account-id",
      "interface-id",
      "srcaddr",
      "dstaddr",
      "srcport",
      "dstport",
      "protocol",
      "packets",
      "bytes",
      "start",
      "end",
      "action",
      "log-status",
      "owner",
      "logGroup",
      "logStream"
    ],
    "dimensionsSpec": {
      "dimensions": [
        { "name": "owner", "type": "string" },
        { "name": "logGroup", "type": "string" },
        { "name": "logStream", "type": "string" },
        { "name": "version", "type": "string" },
        { "name": "account-id", "type": "string" },
        { "name": "interface-id", "type": "string" },
        { "name": "srcaddr", "type": "string" },
        { "name": "dstaddr", "type": "string" },
        { "name": "srcport", "type": "string" },
        { "name": "dstport", "type": "string" },
        { "name": "protocol", "type": "string" },
        { "name": "action", "type": "string" },
        { "name": "log-status", "type": "string" }
      ]
    }
  }
}
```

## GeoIP

Performs IP to location lookups using a MaxMind City database provided by you, the user. For this extension to work,
specify this property in your common.runtime.properties:

```
imply.utility.belt.geoDatabase=/path/to/db.mmdb
```

This functions is available through either Druid expressions (useful for ingest-time transformSpecs) or Druid SQL:

|Function|Description|
|--------|-----------|
|ft_geoip(addr, type)|Look up the "type" attribute of "addr". Type can be one of the types listed below.|

Lookup types supported:

- `lat` - Latitude.
- `lon` - Longitude.
- `geohash9` - Geohash to 9 places of precision, like "wdqcbntdq".
- `city` - City name, like "Beijing".
- `metro` - Metro code, like "819".
- `region` - Name of the most specific subdivision, like "Östergötland County".
- `regionIso` - ISO-3166-2 subdivision code, up to 3 characters long, like "CA".
- `country` - Country name, like "India".
- `countryIso` - ISO-3166-1 country code, 2 characters long, like "DE".
- `continent` - Continent name, like "North America".

Lookup types supported:

- `browser` - Browser type, like "Chrome".
- `browser_version` - Browser version, like "70.0.3538.77".
- `agent_type` - User agent type, like "Browser".
- `agent_category` - User agent category, like "Personal computer".
- `os` - Operating system, like "OS X".
- `platform` - Platform, like "OS X".

## Spatial functions

Operations on spatial data.

|Function|Description|
|--------|-----------|
|st_geohash(lon, lat, maxchars)|Create a [geohash](https://en.wikipedia.org/wiki/Geohash) for the provided longitude and latitude, with length up to maxchars. The maxchars parameter must be a literal between 1 and 12 (inclusive).<br /><br />If longitude is outside of the range -180 (exclusive) to 180 (inclusive), or latitude is outside of the range -90 (exclusive) to 90 (inclusive), returns null. If any inputs are null, returns null.|

## Currency conversion aggregator

### Usage

This extension provides a `currencySum` aggregator that applies a time-based currency conversion
table while performing a sum over an underlying column. For example, if you have a column `usd`
and provide a USD to EUR conversion table, the aggregator's behavior is equivalent to the pseudo-SQL
`SUM(RATE_FOR_DAY(__time) * usd)`.

Example usage in a query, converting USD to EUR for the month of July 2016:

```
{
  "queryType": "timeseries",
  "dataSource": "sample_datasource",
  "intervals": [ "2016-07-01/P1M" ]
  "granularity": "all",
  "aggregations": [
    {
      "type" : "currencySum",
      "name" : "eur",
      "fieldName" : "usd",
      "conversions": {
        "2016-07-01T00:00:00.000Z": 0.899531,
        "2016-07-02T00:00:00.000Z": 0.89699,
        "2016-07-03T00:00:00.000Z": 0.89699,
        "2016-07-04T00:00:00.000Z": 0.897827,
        "2016-07-05T00:00:00.000Z": 0.898707,
        "2016-07-06T00:00:00.000Z": 0.903489,
        "2016-07-07T00:00:00.000Z": 0.902389,
        "2016-07-08T00:00:00.000Z": 0.903849,
        "2016-07-09T00:00:00.000Z": 0.904593,
        "2016-07-10T00:00:00.000Z": 0.904593,
        "2016-07-11T00:00:00.000Z": 0.905133,
        "2016-07-12T00:00:00.000Z": 0.90247,
        "2016-07-13T00:00:00.000Z": 0.902739,
        "2016-07-14T00:00:00.000Z": 0.899944,
        "2016-07-15T00:00:00.000Z": 0.900341,
        "2016-07-16T00:00:00.000Z": 0.905772,
        "2016-07-17T00:00:00.000Z": 0.905772,
        "2016-07-18T00:00:00.000Z": 0.903979,
        "2016-07-19T00:00:00.000Z": 0.904945,
        "2016-07-20T00:00:00.000Z": 0.908306,
        "2016-07-21T00:00:00.000Z": 0.907408,
        "2016-07-22T00:00:00.000Z": 0.908232,
        "2016-07-23T00:00:00.000Z": 0.910631,
        "2016-07-24T00:00:00.000Z": 0.910631,
        "2016-07-25T00:00:00.000Z": 0.910896,
        "2016-07-26T00:00:00.000Z": 0.90943,
        "2016-07-27T00:00:00.000Z": 0.909124,
        "2016-07-28T00:00:00.000Z": 0.902454,
        "2016-07-29T00:00:00.000Z": 0.899434,
        "2016-07-30T00:00:00.000Z": 0.894486,
        "2016-07-31T00:00:00.000Z": 0.894486
      }
    }
  ]
}
```

The conversion table is applied with the following rules:

1. The table is expected to contain key/value pairs where the key is an ISO8601 timestamp,
   and the value is a currency conversion rate.
2. The `__time` column for the row (Druid's builtin timestamp column) is used for evaluating which
   conversion rate to use.
3. Rows with timestamps before the earliest provided conversion time will use the rate 1.0.
4. All other rows use the rate corresponding to the latest conversion table timestamp which is equal
   to or less than the row's timestamp. (i.e. the most recent conversion as of the timestamp in that row)


## IP Address Columns

### IP Address Column

IPv4 and IPv6 addresses can be ingested into specialized column type that stores IP address in their native 128-bit binary format.

```json
{
   "type": "ipAddress",
   "name": "someColumnName"
}
```

### IP Prefix Column

IPv4 and IPv6 prefixes can be ingested into specialized column type that stores IP prefix using the address's native 128-bit binary format plus 8-bit for the size of the prefix.

```json
{
   "type": "ipPrefix",
   "name": "someColumnName"
}
```

### SQL Functions for IP Address and IP Prefix
|Function|Description|
|--------|-----------|
| `IP_PARSE(expr)` | Parses the string into an IP address complex type. Raises `ParseException` if the input string is not a valid IP address | 
| `IP_TRY_PARSE(expr)` | Parses the string into an IP address complex type. Returns null if the input string is not a valid IP address |
| `IP_PREFIX_PARSE(expr)` | Parses the string into an IP prefix complex type. Raises `ParseException` if the input string is not a valid IP address prefix| 
| `IP_PREFIX_TRY_PARSE(expr)` | Parses the string into an IP prefix complex type. Returns null if the input string is not a valid IP address prefix|
| `IP_PREFIX(expr, prefixLengthValue)` | Convert an IP Address complex type `expr` into a prefix block with the length given by `prefixLengthValue`. This returns IP Prefix complex data type. | 
| `IP_HOST(expr)` | Convert an IP Prefix complex type `expr` into host IP address. This returns IP Address complex data type. | 
| `IP_STRINGIFY(expr)` | Stringifies an IP address or an IP prefix column into "compacted" format. IPv4 addresses will print in IPv4 format, not IPv6 | 
| `IP_STRINGIFY(expr, compact)`| Stringifies an IP address or an IP prefix column, into "compacted" format if the `compact` argument is 1, else in "full" format if not. IPv4 addresses will print in IPv4 format. |
| `IP_MATCH(expr1, expr2)` | Check if `expr2` contains or is equal to the address of `expr1`. `expr1` must be an IP address string or an IP address complex type, `expr2` must be a CIDR range string, specific IP address string or IP prefix complex type. Note that `expr1` and `expr2` cannot both be Complex type.| 
| `IP_SEARCH(expr1, expr2)` | Check if `expr2` contains or is equal to the address of `expr1`. `expr1` can be an incomplete IP address string or an IP address complex type, `expr2` can be an incomplete IP address string or IP prefix complex type. If either `expr1` or `expr2` is an incomplete ip address, it will be expanded into all possible ipv4 or ipv6 ranges that starts with the incomplete string. (Note that if the incomplete ip address contains `.`, it will only be expanded into ipv4 ranges, if it contains `:`, it will only be expanded into ipv6 ranges, and if it does not contains both `.` and `:`, then it will be expanded into both ipv4 and ipv6 ranges if possible. Note that `expr1` and `expr2` cannot both be Complex type. | 
