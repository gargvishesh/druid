package io.imply.druid.sql.calcite.external;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.druid.data.input.impl.InlineInputSource;
import org.apache.druid.data.input.impl.JsonInputFormat;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.junit.Assert;
import org.junit.Test;

public class PolarisExternalTableSpecTest
{
  private static final ObjectMapper MAPPER = new ObjectMapper();

  @Test
  public void testSerde() throws JsonProcessingException
  {
    InlineInputSource inputSource = new InlineInputSource("a\nc");
    JsonInputFormat inputFormat = new JsonInputFormat(null, null, null, null, null);
    RowSignature rowSignature = RowSignature.builder()
        .add("col_double", ColumnType.DOUBLE)
        .add("col_float", ColumnType.FLOAT)
        .add("col_long", ColumnType.LONG)
        .add("col_string", ColumnType.STRING)
        .build();
    PolarisExternalTableSpec spec = new PolarisExternalTableSpec(inputSource, inputFormat, rowSignature);

    PolarisExternalTableSpec specSerde =
        MAPPER.readValue(MAPPER.writeValueAsString(spec), PolarisExternalTableSpec.class);

    Assert.assertEquals(spec, specSerde);
  }
}
