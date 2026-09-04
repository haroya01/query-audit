package io.queryaudit.junit5;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.queryaudit.core.analyzer.IndexMetadataProvider;
import io.queryaudit.core.model.IndexMetadata;
import io.queryaudit.core.provenance.AuditCapability;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import javax.sql.DataSource;
import org.junit.jupiter.api.Test;

class IndexMetadataCapabilityTest {
  @Test
  void failedMatchingProviderDoesNotFallBackToAnApparentlySuccessfulJpaScan() throws Exception {
    IndexMetadataCollector collector = new IndexMetadataCollector(List.of(new FailingProvider()));
    IndexMetadataCollector.Result result = collector.collectWithCapabilities(dataSource());
    assertThat(result.metadata()).isNull();
    assertThat(result.dialect()).isEqualTo("h2");
    assertThat(result.capability().state()).isEqualTo(AuditCapability.State.FAILED);
    assertThat(result.failure()).isEqualTo("SQLException");
  }

  @Test
  void aSuccessfulEmptyMetadataResultIsStillAnAvailableCapability() throws Exception {
    IndexMetadataCollector collector = new IndexMetadataCollector(List.of(new EmptyProvider()));
    IndexMetadataCollector.Result result = collector.collectWithCapabilities(dataSource());
    assertThat(result.metadata()).isNotNull();
    assertThat(result.capability().state()).isEqualTo(AuditCapability.State.AVAILABLE);
    assertThat(result.failure()).isNull();
  }

  @Test
  void failedJdbcMetadataIsNotReportedAsAnUnsupportedDatabase() throws Exception {
    DataSource dataSource = mock(DataSource.class);
    when(dataSource.getConnection()).thenThrow(new SQLException("private connection details"));
    IndexMetadataCollector.Result result =
        new IndexMetadataCollector(List.of()).collectWithCapabilities(dataSource);
    assertThat(result.capability().state()).isEqualTo(AuditCapability.State.FAILED);
    assertThat(result.dialect()).isNull();
    assertThat(result.failure()).doesNotContain("private connection details");
  }

  private static DataSource dataSource() throws Exception {
    DataSource dataSource = mock(DataSource.class);
    Connection connection = mock(Connection.class);
    DatabaseMetaData metadata = mock(DatabaseMetaData.class);
    when(dataSource.getConnection()).thenReturn(connection);
    when(connection.getMetaData()).thenReturn(metadata);
    when(metadata.getDatabaseProductName()).thenReturn("H2");
    return dataSource;
  }

  private static class FailingProvider implements IndexMetadataProvider {
    public String supportedDatabase() {
      return "h2";
    }

    public IndexMetadata getIndexMetadata(Connection connection) throws SQLException {
      throw new SQLException("cannot read catalog");
    }
  }

  private static class EmptyProvider implements IndexMetadataProvider {
    public String supportedDatabase() {
      return "h2";
    }

    public IndexMetadata getIndexMetadata(Connection connection) {
      return new IndexMetadata(Map.of());
    }
  }
}
