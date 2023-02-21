package io.imply.druid.sql.calcite.external;

import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.fasterxml.jackson.databind.Module;
import com.google.common.collect.ImmutableList;
import com.google.inject.Binder;
import org.apache.druid.catalog.model.CatalogUtils;
import org.apache.druid.common.aws.AWSModule;
import org.apache.druid.data.input.InputFormat;
import org.apache.druid.data.input.impl.CsvInputFormat;
import org.apache.druid.data.input.impl.JsonInputFormat;
import org.apache.druid.data.input.s3.S3InputSource;
import org.apache.druid.data.input.s3.S3InputSourceDruidModule;
import org.apache.druid.guice.DruidInjectorBuilder;
import org.apache.druid.initialization.DruidModule;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.sql.calcite.CalciteIngestionDmlTest;
import org.apache.druid.sql.calcite.external.ExternalDataSource;
import org.apache.druid.sql.calcite.external.Externals;
import org.apache.druid.sql.calcite.filtration.Filtration;
import org.apache.druid.sql.calcite.util.CalciteTests;
import org.apache.druid.sql.guice.SqlBindings;
import org.apache.druid.storage.s3.NoopServerSideEncryption;
import org.apache.druid.storage.s3.S3InputDataConfig;
import org.apache.druid.storage.s3.S3StorageDruidModule;
import org.apache.druid.storage.s3.ServerSideEncryptingAmazonS3;
import org.easymock.EasyMock;
import org.junit.Test;

import javax.annotation.Nullable;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Collections;
import java.util.List;

public class PolarisTableFunctionTest extends CalciteIngestionDmlTest
{
  private static final String VALID_SOURCE_ARG = "<polaris_source>";

  protected static URI toURI(String uri)
  {
    try {
      return new URI(uri);
    }
    catch (URISyntaxException e) {
      throw new ISE("Bad URI: %s", uri);
    }
  }

  protected final S3InputSource s3InputSource = new S3InputSource(
      S3_SERVICE,
      SERVER_SIDE_ENCRYPTING_AMAZON_S3_BUILDER,
      S3_INPUT_DATA_CONFIG,
      null,
      CatalogUtils.stringListToUriList(Collections.singletonList("s3://foo/bar/file.csv")),
      null,
      null,
      null,
      null,
      null,
      null,
      null
  );

  protected final InputFormat csvInputFormat = new CsvInputFormat(
      ImmutableList.of("x", "y", "z"),
      null,
      false,
      false,
      0
  );

  protected final InputFormat jsonInputFormat = new JsonInputFormat(null, null, null, null, null);
  protected final RowSignature rowSignature = RowSignature
      .builder()
      .add("x", ColumnType.STRING)
      .add("y", ColumnType.STRING)
      .add("z", ColumnType.LONG)
      .build();

  protected final ExternalDataSource polarisSourceDataSource = new ExternalDataSource(
      s3InputSource,
      csvInputFormat,
      rowSignature
  );

  protected final ExternalDataSource polarisUploadedDataSource = new ExternalDataSource(
      s3InputSource,
      jsonInputFormat,
      rowSignature
  );

  public static final AmazonS3Client S3_CLIENT = EasyMock.createMock(AmazonS3Client.class);
  public static final AmazonS3ClientBuilder AMAZON_S3_CLIENT_BUILDER = AmazonS3Client.builder();
  public static final ServerSideEncryptingAmazonS3.Builder SERVER_SIDE_ENCRYPTING_AMAZON_S3_BUILDER =
      EasyMock.createMock(ServerSideEncryptingAmazonS3.Builder.class);
  public static final ServerSideEncryptingAmazonS3 S3_SERVICE = new ServerSideEncryptingAmazonS3(
      S3_CLIENT,
      new NoopServerSideEncryption()
  );
  public static final S3InputDataConfig S3_INPUT_DATA_CONFIG = new S3InputDataConfig();

  protected final PolarisTableFunctionResolver resolver = new PolarisTableFunctionResolver()
  {
    @Override
    public PolarisExternalTableSpec resolve(@Nullable PolarisTableFunctionSpec fn)
    {
      if (fn instanceof PolarisSourceOperatorConversion.PolarisSourceFunctionSpec) {
        PolarisSourceOperatorConversion.PolarisSourceFunctionSpec polarisSourceFnSpec =
            (PolarisSourceOperatorConversion.PolarisSourceFunctionSpec) fn;
        if (polarisSourceFnSpec.getSource().equals(VALID_SOURCE_ARG)) {
          return new PolarisExternalTableSpec(
              polarisSourceDataSource.getInputSource(),
              polarisSourceDataSource.getInputFormat(),
              polarisSourceDataSource.getSignature()
          );
        }
      }

      if (fn instanceof PolarisUploadedInputSourceDefn.PolarisUploadedFunctionSpec) {
        return new PolarisExternalTableSpec(
            polarisSourceDataSource.getInputSource(),
            null,
            null
        );
      }

      throw new IAE(StringUtils.format("Undefined resolving of polarisFunction %s", fn));
    }
  };

  @Override
  public void configureGuice(DruidInjectorBuilder builder)
  {
    super.configureGuice(builder);

    builder.addModule(new DruidModule() {

      @Override
      public List<? extends Module> getJacksonModules()
      {
        // We want this module to bring input sources along for the ride.
        return new S3InputSourceDruidModule().getJacksonModules();
      }

      @Override
      public void configure(Binder binder)
      {
        // We want this module to bring S3InputSourceDruidModule along for the ride.
        binder.install(new AWSModule());
        binder.install(new S3InputSourceDruidModule());
        binder.install(new S3StorageDruidModule());

        binder.bind(PolarisTableFunctionResolver.class).toInstance(resolver);

        // Set up the POLARIS macros.
        SqlBindings.addOperatorConversion(binder, PolarisSourceOperatorConversion.class);
        SqlBindings.addOperatorConversion(binder, PolarisUploadedOperatorConversion.class);
      }
    });
  }

  @Test
  public void test_PolarisSource_withProperArgs_valid()
  {
    testIngestionQuery()
        .sql(StringUtils.format("INSERT INTO dst SELECT *\n" +
                        "FROM TABLE(POLARIS_SOURCE(source => '%s'))\n" +
                        "PARTITIONED BY ALL TIME", VALID_SOURCE_ARG))
        .authentication(CalciteTests.SUPER_USER_AUTH_RESULT)
        .expectTarget("dst", polarisSourceDataSource.getSignature())
        .expectResources(dataSourceWrite("dst"), Externals.EXTERNAL_RESOURCE_ACTION)
        .expectQuery(
            newScanQueryBuilder()
                .dataSource(polarisSourceDataSource)
                .intervals(querySegmentSpec(Filtration.eternity()))
                .columns("x", "y", "z")
                .context(CalciteIngestionDmlTest.PARTITIONED_BY_ALL_TIME_QUERY_CONTEXT)
                .build()
        )
        .expectLogicalPlanFrom("polarisSourceExtern")
        .verify();
  }

  @Test
  public void test_PolarisUploaded_withProperArgs_valid()
  {
    testIngestionQuery()
        .sql("INSERT INTO dst SELECT *\n" +
             "FROM TABLE(POLARIS_UPLOADED(\n"
             + "                          files => ARRAY['zach_test.json'],"
             + "                          format => 'json'))\n" +
             "     EXTEND (x VARCHAR, y VARCHAR, z BIGINT)\n" +
             "PARTITIONED BY ALL TIME")
        .authentication(CalciteTests.SUPER_USER_AUTH_RESULT)
        .expectTarget("dst", polarisUploadedDataSource.getSignature())
        .expectResources(dataSourceWrite("dst"), Externals.EXTERNAL_RESOURCE_ACTION)
        .expectQuery(
            newScanQueryBuilder()
                .dataSource(polarisUploadedDataSource)
                .intervals(querySegmentSpec(Filtration.eternity()))
                .columns("x", "y", "z")
                .context(CalciteIngestionDmlTest.PARTITIONED_BY_ALL_TIME_QUERY_CONTEXT)
                .build()
        )
        .expectLogicalPlanFrom("polarisUploadedExtern")
        .verify();
  }
}
