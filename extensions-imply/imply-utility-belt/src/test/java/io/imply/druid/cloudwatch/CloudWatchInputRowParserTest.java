/*
 * Copyright (c) 2019 Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 *  of Imply Data, Inc.
 */

package io.imply.druid.cloudwatch;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.BaseEncoding;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.MapBasedInputRow;
import org.apache.druid.data.input.impl.DelimitedParseSpec;
import org.apache.druid.data.input.impl.DimensionSchema;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.LongDimensionSchema;
import org.apache.druid.data.input.impl.StringDimensionSchema;
import org.apache.druid.data.input.impl.TimestampSpec;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.DateTimes;
import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.List;

public class CloudWatchInputRowParserTest
{
  @Test
  public void testWithFlowLogs()
  {
    final CloudWatchInputRowParser parser = new CloudWatchInputRowParser(
        new DelimitedParseSpec(
            new TimestampSpec("start", "posix", null),
            new DimensionsSpec(
                ImmutableList.<DimensionSchema>builder()
                    // CloudWatch dimensions
                    .add(StringDimensionSchema.create("owner"))
                    .add(StringDimensionSchema.create("logGroup"))
                    .add(StringDimensionSchema.create("logStream"))

                    // VPC Flow dimensions
                    .add(StringDimensionSchema.create("version"))
                    .add(StringDimensionSchema.create("account-id"))
                    .add(StringDimensionSchema.create("interface-id"))
                    .add(StringDimensionSchema.create("srcaddr"))
                    .add(StringDimensionSchema.create("dstaddr"))
                    .add(StringDimensionSchema.create("srcport"))
                    .add(StringDimensionSchema.create("dstport"))
                    .add(StringDimensionSchema.create("protocol"))
                    .add(new LongDimensionSchema("packets"))
                    .add(new LongDimensionSchema("bytes"))
                    .add(new LongDimensionSchema("start"))
                    .add(new LongDimensionSchema("end"))
                    .build(),
                null,
                null
            ),
            " ",
            null,
            ImmutableList.of(
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

                // Dummy values to shut up the validator.
                "owner",
                "logGroup",
                "logStream"
            ),
            false,
            0
        ),
        new DefaultObjectMapper()
    );

    final ByteBuffer input = ByteBuffer.wrap(
        BaseEncoding.base64().decode(
            "H4sIAAAAAAAAAL2X3W4bNxCFX4XQtbWYH3LIyZ3hqAFaFC1q3xVBocbrQIBsGZKcoAjy7j2UbFderQ1vIxnWQhR3xeGnGZ45/"
            + "ja6bler6ef24p/bdvRu9P704vSvXyfn56cfJqOT0eLrTbvEtJiXnNw0GmN6vvj8Ybm4u8WdL7efxlfzxdcx5lbjy/Zqejdfb"
            + "x85Xy/b6TWeaW9m4/ZyeiWx9fF0Psft1d3fq0/L2e16trj5aTZft8vV6N2fz6z2cbPc5Et7s65PfRvNLrGqRsIrsWoRIdGYx"
            + "NgsZqXCKoU5k+GPse+cvYiKJsqC6OsZqNfTawBw/Za6xUhEJw+/RkUOu9BhlyHE1IjkRlLExYGzNMoNM2YlJKKsQdU5WOAQK"
            + "fwX43HoJfwx+XlydhF++2X0/eTHkPQASE8ZHj4pJkJ0TjmYZgeQJAm5RN+nisS5hNOzs8nvB6GKB6RKjXUI3ZGCVHIEWdAUu"
            + "MjxkdIBkTapecq0SdF9tgKLhcw7JXcsKDt+9ZWH6nMPiWM5OlM+GlOtxG3ZbSpwU3vKzOnoTOWNiq9s8yRYgwdDiWUTix4ta"
            + "iHSYqhmjZmFNbInAl9mTMdSP2Xvhco0BMqlMQB5bva1nF2QGSLrijlCPA7TS2I+kKjQAYjYQUC4LDcuXaYYVYNgOXkrJj4EE"
            + "+UGhd0Imi+idKA0xgQotf/NlIRLil7cCd/xiEEmzCi5JeLI4C1oEpGTsVPp9xGFyxCmgt1zbAq8hNJenuAxQhL4iQ4TgjwO8"
            + "0tMZk7qdbOcNbGRVDQnSiTRTahowr3EiJ6Rp34jUcqQPJkhRUgTpWYvSwJRqJ0JkrVFeswNYjwMXQ+J1O8ihiFBBKrPQ44qy"
            + "Z7wpQRQVErqpum1TLngJKmrCAZsUG1QShFcLlxfJXvCSauhgN5vI1yHlJ5AHiRC9uBjc+6WnlmCmN8TwfHtYDwMX3SwQ4n6P"
            + "cQwIs4OmioOEIq904SqgGcV7dTdq4kcXdRKHWPTcPjYOcMGZ9GSgAvlS9iZG6LUciz9DgIxhhBFawjZaWBO98QBIYIV6valS"
            + "nHfaelFvRsK1G8fBgI9PTixMUHfJeh6rqqwvTAM2Z7hed45DOXpdw7DeDoETw3f2/J4v28YmJ9UfYNt/F3a60coR/xza7RFi"
            + "juH6JU1h76paONR4BggBizRUp3wKOixVuAhFFxoHRsZJ+/zDXCQ+MEHuLuq2zhChnfqJgktA/6gowqbCLt2dQfo4/d/AXFid"
            + "ocyEQAA"
        )
    );

    final List<InputRow> rows = parser.parseBatch(input);

    Assert.assertEquals(
        new MapBasedInputRow(
            DateTimes.of("2018-05-12T15:40:44.000Z"),
            parser.getParseSpec().getDimensionsSpec().getDimensionNames(),
            ImmutableMap.<String, Object>builder()
                .put("owner", "269875963461")
                .put("logGroup", "vpc-flow-logs-default")
                .put("logStream", "eni-edaf24e9-all")
                .put("version", "2")
                .put("account-id", "269875963461")
                .put("interface-id", "eni-edaf24e9")
                .put("srcaddr", "45.227.254.251")
                .put("dstaddr", "172.31.11.222")
                .put("srcport", "50073")
                .put("dstport", "3391")
                .put("protocol", "6")
                .put("packets", "1")
                .put("bytes", "40")
                .put("start", "1526139644")
                .put("end", "1526139698")
                .put("action", "REJECT")
                .put("log-status", "OK")
                .build()
        ),
        rows.get(0)
    );

    Assert.assertEquals(20, rows.size());
  }
}
