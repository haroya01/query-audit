package io.queryaudit.junit5.integration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

import io.queryaudit.junit5.BooleanOverride;
import io.queryaudit.junit5.ExpectQueries;
import io.queryaudit.junit5.QueryAudit;
import io.queryaudit.junit5.QueryAuditExtension;
import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.Statement;
import java.util.Optional;
import javax.sql.DataSource;
import org.h2.jdbcx.JdbcDataSource;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

@DisplayName("Plain JUnit DataSource integration")
class PlainJUnitDataSourceIntegrationTest {

  @Test
  @DisplayName("a query contract sees SQL executed through the documented static field")
  void capturesQueriesFromStaticDataSource() throws Exception {
    DataSource original = newH2DataSource();
    PlainJUnitFixture.dataSource = original;
    QueryAuditExtension extension = new QueryAuditExtension();
    MethodContext context = new MethodContext(PlainJUnitFixture.class, "auditedQuery");

    try {
      extension.beforeEach(context);
      extension.beforeTestExecution(context);
      try (Connection connection = PlainJUnitFixture.dataSource.getConnection();
          Statement statement = connection.createStatement()) {
        statement.execute("SELECT 1");
      }
      extension.afterTestExecution(context);

      assertThatCode(() -> extension.afterEach(context)).doesNotThrowAnyException();
    } finally {
      extension.afterAll(context);
    }

    assertThat(PlainJUnitFixture.dataSource).isSameAs(original);
  }

  private static DataSource newH2DataSource() {
    JdbcDataSource dataSource = new JdbcDataSource();
    dataSource.setURL("jdbc:h2:mem:plain_junit_extension;DB_CLOSE_DELAY=-1");
    return dataSource;
  }

  private static class MethodContext extends StubExtensionContext {

    private final Method method;

    MethodContext(Class<?> testClass, String methodName) throws NoSuchMethodException {
      super(testClass);
      method = testClass.getDeclaredMethod(methodName);
    }

    @Override
    public Optional<Method> getTestMethod() {
      return Optional.of(method);
    }

    @Override
    public String getDisplayName() {
      return method.getName();
    }
  }
}

@QueryAudit(autoOpenReport = BooleanOverride.FALSE)
class PlainJUnitFixture {

  static DataSource dataSource;

  @ExpectQueries(select = 1)
  void auditedQuery() {}
}
