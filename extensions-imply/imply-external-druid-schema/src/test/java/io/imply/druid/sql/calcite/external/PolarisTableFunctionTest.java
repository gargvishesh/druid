/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.sql.calcite.external;

import com.amazonaws.services.s3.AmazonS3Client;
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
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.sql.SqlPlanningException;
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
import org.hamcrest.CoreMatchers;
import org.junit.Test;
import org.junit.internal.matchers.ThrowableMessageMatcher;

import javax.annotation.Nullable;
import java.util.Collections;
import java.util.List;

public class PolarisTableFunctionTest extends CalciteIngestionDmlTest
{
  private static final String VALID_SOURCE_ARG = "<valid polaris_source>";
  private static final String INVALID_SOURCE_ARG = "<invalid polaris_source>";

  private static final String VALID_POLARIS_FILE = "valid_file.json";
  private static final String INVALID_POLARIS_FILE = "invalid_file.json";

  private static final String VALID_POLARIS_S3_CONN_NAME = "valid-connection";
  private static final String INVALID_POLARIS_S3_CONN_NAME = "invalid-connection";

  private static final String RESOLVER_FAILURE_MESSAGE = "failure occurred when resolving polaris source";

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

  protected final ExternalDataSource polarisS3ConnectionDataSource = new ExternalDataSource(
      s3InputSource,
      jsonInputFormat,
      rowSignature
  );

  public static final AmazonS3Client S3_CLIENT = EasyMock.createMock(AmazonS3Client.class);
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
        if (polarisSourceFnSpec.getSource().equals(INVALID_SOURCE_ARG)) {
          throw new RuntimeException(RESOLVER_FAILURE_MESSAGE);
        }
      }

      if (fn instanceof PolarisUploadedInputSourceDefn.PolarisUploadedFunctionSpec) {
        PolarisUploadedInputSourceDefn.PolarisUploadedFunctionSpec polarisUploadedFnSpec =
            (PolarisUploadedInputSourceDefn.PolarisUploadedFunctionSpec) fn;
        if (polarisUploadedFnSpec.getFiles().contains(INVALID_POLARIS_FILE)) {
          throw new RuntimeException(RESOLVER_FAILURE_MESSAGE);
        }
        if (polarisUploadedFnSpec.getFiles().contains(VALID_POLARIS_FILE)) {
          return new PolarisExternalTableSpec(
              polarisSourceDataSource.getInputSource(),
              null,
              null
          );
        }
      }

      if (fn instanceof PolarisS3ConnectionInputSourceDefn.PolarisS3ConnectionFunctionSpec) {
        PolarisS3ConnectionInputSourceDefn.PolarisS3ConnectionFunctionSpec polarisS3ConnFunctionSpec =
            (PolarisS3ConnectionInputSourceDefn.PolarisS3ConnectionFunctionSpec) fn;
        if (polarisS3ConnFunctionSpec.getConnectionName().contains(INVALID_POLARIS_S3_CONN_NAME)) {
          throw new RuntimeException(RESOLVER_FAILURE_MESSAGE);
        }
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
        SqlBindings.addOperatorConversion(binder, PolarisS3ConnectionOperatorConversion.class);
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
  public void test_PolarisSource_withExtend_validationError()
  {
    testIngestionQuery()
        .sql(StringUtils.format("INSERT INTO dst SELECT *\n" +
                                "FROM TABLE(POLARIS_SOURCE(source => '%s'))\n" +
                                "     EXTEND (x VARCHAR, y VARCHAR, z BIGINT)\n" +
                                "PARTITIONED BY ALL TIME", VALID_SOURCE_ARG))
        .expectValidationError(
            CoreMatchers.allOf(
                CoreMatchers.instanceOf(SqlPlanningException.class),
                ThrowableMessageMatcher.hasMessage(CoreMatchers.containsString(
                    "POLARIS_SOURCE does not support the EXTEND clause."))
            ))
        .verify();
  }

  @Test
  public void test_PolarisSource_resolvingFailure_validationError()
  {
    testIngestionQuery()
        .sql(StringUtils.format("INSERT INTO dst SELECT *\n" +
                                "FROM TABLE(POLARIS_SOURCE(source => '%s'))\n" +
                                "PARTITIONED BY ALL TIME", INVALID_SOURCE_ARG))
        .expectValidationError(
            CoreMatchers.allOf(
                CoreMatchers.instanceOf(SqlPlanningException.class),
                ThrowableMessageMatcher.hasMessage(CoreMatchers.containsString(RESOLVER_FAILURE_MESSAGE))
            ))
        .verify();
  }

  @Test
  public void test_PolarisUploaded_withProperArgs_valid()
  {
    testIngestionQuery()
        .sql(StringUtils.format("INSERT INTO dst SELECT *\n" +
             "FROM TABLE(POLARIS_UPLOADED(\n" +
             "                          files => ARRAY['%s']," +
             "                          format => 'json'))\n" +
             "     EXTEND (x VARCHAR, y VARCHAR, z BIGINT)\n" +
             "PARTITIONED BY ALL TIME", VALID_POLARIS_FILE))
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

  @Test
  public void test_PolarisUploaded_withoutFormatArg_validationError()
  {
    testIngestionQuery()
        .sql(StringUtils.format("INSERT INTO dst SELECT *\n" +
                                "FROM TABLE(POLARIS_UPLOADED(\n" +
                                "                          files => ARRAY['%s']))\n" +
                                "     EXTEND (x VARCHAR, y VARCHAR, z BIGINT)\n" +
                                "PARTITIONED BY ALL TIME", VALID_POLARIS_FILE))
        .expectValidationError(
            CoreMatchers.allOf(
                CoreMatchers.instanceOf(SqlPlanningException.class),
                ThrowableMessageMatcher.hasMessage(CoreMatchers.containsString(
                    "Must provide a value for the [format] parameter"))
            ))
        .verify();
  }

  @Test
  public void test_PolarisUploaded_withoutfilesArg_validationError()
  {
    testIngestionQuery()
        .sql("INSERT INTO dst SELECT *\n" +
                                "FROM TABLE(POLARIS_UPLOADED(\n" +
                                "                          format => 'json'))\n" +
                                "     EXTEND (x VARCHAR, y VARCHAR, z BIGINT)\n" +
                                "PARTITIONED BY ALL TIME")
        .expectValidationError(
            CoreMatchers.allOf(
                CoreMatchers.instanceOf(SqlPlanningException.class),
                ThrowableMessageMatcher.hasMessage(CoreMatchers.containsString(
                    "Must provide a non-empty value for the [files] parameter"))
            ))
        .verify();
  }

  @Test
  public void test_PolarisUploaded_resolvingFailure_validationError()
  {
    testIngestionQuery()
        .sql(StringUtils.format("INSERT INTO dst SELECT *\n" +
                                "FROM TABLE(POLARIS_UPLOADED(\n" +
                                "                          files => ARRAY['%s']," +
                                "                          format => 'json'))\n" +
                                "     EXTEND (x VARCHAR, y VARCHAR, z BIGINT)\n" +
                                "PARTITIONED BY ALL TIME", INVALID_POLARIS_FILE))
        .expectValidationError(
            CoreMatchers.allOf(
                CoreMatchers.instanceOf(SqlPlanningException.class),
                ThrowableMessageMatcher.hasMessage(CoreMatchers.containsString(RESOLVER_FAILURE_MESSAGE))
            ))
        .verify();
  }

  @Test
  public void test_PolarisS3Connection_withProperArgs_valid()
  {
    testIngestionQuery()
        .sql(StringUtils.format("INSERT INTO dst SELECT *\n" +
                                "FROM TABLE(POLARIS_S3_CONNECTION(\n" +
                                "                          connectionName => '%s'," +
                                "                          uris => ARRAY['%s']," +
                                "                          format => 'json'))\n" +
                                "     EXTEND (x VARCHAR, y VARCHAR, z BIGINT)\n" +
                                "PARTITIONED BY ALL TIME", VALID_POLARIS_S3_CONN_NAME, "s3://foo/bar.json"))
        .authentication(CalciteTests.SUPER_USER_AUTH_RESULT)
        .expectTarget("dst", polarisS3ConnectionDataSource.getSignature())
        .expectResources(dataSourceWrite("dst"), Externals.EXTERNAL_RESOURCE_ACTION)
        .expectQuery(
            newScanQueryBuilder()
                .dataSource(polarisS3ConnectionDataSource)
                .intervals(querySegmentSpec(Filtration.eternity()))
                .columns("x", "y", "z")
                .context(CalciteIngestionDmlTest.PARTITIONED_BY_ALL_TIME_QUERY_CONTEXT)
                .build()
        )
        .verify();
  }

  @Test
  public void test_PolarisS3connection_withoutConnectionNameArg_validationError()
  {
    testIngestionQuery()
        .sql(StringUtils.format("INSERT INTO dst SELECT *\n" +
                                "FROM TABLE(POLARIS_S3_CONNECTION(\n" +
                                "                          objects => ARRAY['s3://foo/bar.json']," +
                                "                          format => 'json'))\n" +
                                "     EXTEND (x VARCHAR, y VARCHAR, z BIGINT)\n" +
                                "PARTITIONED BY ALL TIME"))
        .expectValidationError(
            CoreMatchers.allOf(
                CoreMatchers.instanceOf(SqlPlanningException.class),
                ThrowableMessageMatcher.hasMessage(CoreMatchers.containsString(
                    "Must provide a value for the [connectionName] parameter"))
            ))
        .verify();
  }

  @Test
  public void test_PolarisS3connection_withoutAnyObjectsArg_validationError()
  {
    testIngestionQuery()
        .sql(StringUtils.format("INSERT INTO dst SELECT *\n" +
                                "FROM TABLE(POLARIS_S3_CONNECTION(\n" +
                                "                          connectionName => '%s'," +
                                "                          format => 'json'))\n" +
                                "     EXTEND (x VARCHAR, y VARCHAR, z BIGINT)\n" +
                                "PARTITIONED BY ALL TIME", VALID_POLARIS_S3_CONN_NAME))
        .expectValidationError(
            CoreMatchers.allOf(
                CoreMatchers.instanceOf(SqlPlanningException.class),
                ThrowableMessageMatcher.hasMessage(CoreMatchers.containsString(
                    "Must provide a non-empty value for one of [uris, prefixes, objects, pattern] parameters"))
            ))
        .verify();
  }

  @Test
  public void test_PolarisS3connection_resolvingFailure_validationError()
  {
    testIngestionQuery()
        .sql(StringUtils.format("INSERT INTO dst SELECT *\n" +
                                "FROM TABLE(POLARIS_S3_CONNECTION(\n" +
                                "                          connectionName => '%s'," +
                                "                          uris => ARRAY['s3://foo/bar.json']," +
                                "                          format => 'json'))\n" +
                                "     EXTEND (x VARCHAR, y VARCHAR, z BIGINT)\n" +
                                "PARTITIONED BY ALL TIME", INVALID_POLARIS_S3_CONN_NAME))
        .expectValidationError(
            CoreMatchers.allOf(
                CoreMatchers.instanceOf(SqlPlanningException.class),
                ThrowableMessageMatcher.hasMessage(CoreMatchers.containsString(RESOLVER_FAILURE_MESSAGE))
            ))
        .verify();
  }
}
