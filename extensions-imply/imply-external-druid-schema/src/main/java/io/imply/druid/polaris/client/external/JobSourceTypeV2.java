package io.imply.druid.polaris.client.external;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;

/**
 * Source of input data for an ingestion job:   * &#x60;connection&#x60; - The job input comes from data accessed through a connection. Requires a valid                    [Polaris connection](#tag/connections).   * &#x60;s3&#x60; - The job input comes from data accessed through a [connection](#tag/connections) to Amazon S3.   * &#x60;table&#x60; - The job input is an existing Polaris table.   * &#x60;uploaded&#x60; - The job input is a set of files uploaded to Imply Polaris.
 */
public enum JobSourceTypeV2 {

  CONNECTION("connection"),

  S3("s3"),

  TABLE("table"),

  UPLOADED("uploaded");

  private String value;

  public String value() {
    return value;
  }

  JobSourceTypeV2(String value) {
    this.value = value;
  }

  @Override
  @JsonValue
  public String toString() {
    return String.valueOf(value);
  }

  @JsonCreator
  public static JobSourceTypeV2 fromValue(String value) {
    for (JobSourceTypeV2 b : JobSourceTypeV2.values()) {
      if (String.valueOf(b.value).replace('-', '_').equalsIgnoreCase(String.valueOf(value).replace('-', '_'))) {
        return b;
      }
    }
    throw new IllegalArgumentException("Unexpected value '" + value + "'");
  }
}
