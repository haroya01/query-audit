package io.queryaudit.junit5;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

import io.queryaudit.core.interceptor.QueryInterceptor;
import java.sql.Connection;
import java.sql.Statement;
import javax.sql.DataSource;
import org.h2.jdbcx.JdbcDataSource;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtensionConfigurationException;
import org.junit.jupiter.api.extension.ExtensionContext;

@DisplayName("Plain JUnit DataSource capture")
class PlainJUnitDataSourceCaptureTest {

  @AfterEach
  void restoreFixture() {
    MutableDataSourceFixture.dataSource = newH2DataSource("cleanup");
    QueryAuditDataSourceStore.clear();
  }

  @Test
  @DisplayName("queries executed through a mutable static field reach the interceptor")
  void capturesQueriesThroughStaticField() throws Exception {
    DataSource original = newH2DataSource("capture");
    MutableDataSourceFixture.dataSource = original;
    DataSourceResolver resolver = new DataSourceResolver();
    QueryInterceptor interceptor = new QueryInterceptor();

    DataSourceResolver.ResolvedDataSource resolved =
        resolver.resolve(contextFor(MutableDataSourceFixture.class));
    Runnable cleanup = resolver.hookInterceptor(resolved, interceptor);

    assertThat(MutableDataSourceFixture.dataSource).isNotSameAs(original);
    interceptor.start();
    try (Connection connection = MutableDataSourceFixture.dataSource.getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("SELECT 1");
    } finally {
      interceptor.stop();
    }

    assertThat(interceptor.getRecordedQueries())
        .singleElement()
        .satisfies(query -> assertThat(query.sql()).isEqualTo("SELECT 1"));

    cleanup.run();
    assertThat(MutableDataSourceFixture.dataSource).isSameAs(original);
  }

  @Test
  @DisplayName("a final raw DataSource fails with setup guidance")
  void rejectsFinalRawDataSource() {
    DataSourceResolver resolver = new DataSourceResolver();
    DataSourceResolver.ResolvedDataSource resolved =
        resolver.resolve(contextFor(FinalDataSourceFixture.class));

    assertThatThrownBy(() -> resolver.hookInterceptor(resolved, new QueryInterceptor()))
        .isInstanceOf(ExtensionConfigurationException.class)
        .hasMessageContaining("FinalDataSourceFixture.dataSource is final")
        .hasMessageContaining("mutable javax.sql.DataSource field");
  }

  private static ExtensionContext contextFor(Class<?> testClass) {
    ExtensionContext context = mock(ExtensionContext.class);
    doReturn(testClass).when(context).getRequiredTestClass();
    return context;
  }

  private static DataSource newH2DataSource(String databaseName) {
    JdbcDataSource dataSource = new JdbcDataSource();
    dataSource.setURL("jdbc:h2:mem:" + databaseName + ";DB_CLOSE_DELAY=-1");
    return dataSource;
  }

  private static class MutableDataSourceFixture {
    static DataSource dataSource = newH2DataSource("mutable");
  }

  private static class FinalDataSourceFixture {
    static final DataSource dataSource = newH2DataSource("final");
  }
}
