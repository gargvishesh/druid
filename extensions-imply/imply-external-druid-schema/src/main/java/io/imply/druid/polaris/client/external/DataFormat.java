package io.imply.druid.polaris.client.external;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;

/**
 * Data format:   * &#x60;avro_ocf&#x60; - Avro OCF (Object Container Format) for batch ingestion   * &#x60;avro_stream&#x60; - Avro for stream ingestion   * &#x60;csv&#x60; - Delimiter-separated data including CSV and TSV   * &#x60;nd-json&#x60; - Newline-delimited JSON (one JSON record per line)   * &#x60;orc&#x60; - ORC format   * &#x60;parquet&#x60; - Parquet format   * &#x60;protobuf&#x60; - Protobuf format
 */
public enum DataFormat {

  AVRO_OCF("avro_ocf"),

  AVRO_STREAM("avro_stream"),

  CSV("csv"),

  ND_JSON("nd-json"),

  ORC("orc"),

  PARQUET("parquet"),

  PROTOBUF("protobuf");

  private String value;

  public String value() {
    return value;
  }

  DataFormat(String value) {
    this.value = value;
  }

  @Override
  @JsonValue
  public String toString() {
    return String.valueOf(value);
  }

  @JsonCreator
  public static DataFormat fromValue(String value) {
    for (DataFormat b : DataFormat.values()) {
      if (String.valueOf(b.value).replace('-', '_').equalsIgnoreCase(String.valueOf(value).replace('-', '_'))) {
        return b;
      }
    }
    throw new IllegalArgumentException("Unexpected value '" + value + "'");
  }
}
