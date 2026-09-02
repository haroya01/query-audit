package io.queryaudit.junit5.nohibernate;

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

/**
 * Regression test for issue #248: a plain JDBC audit must not require Hibernate on the runtime
 * classpath.
 *
 * <p>This class lives in the {@code noHibernateTest} source set (see {@code build.gradle}), which
 * deliberately excludes hibernate-core and jakarta.persistence-api. The main {@code test} source
 * set can't catch this class of regression — it pulls Hibernate in transitively via
 * spring-boot-starter-data-jpa, which is exactly how the original bug (a {@code LazyLoadTracker}
 * that directly implemented Hibernate's event-listener SPIs) shipped without a failing test: every
 * "plain JDBC" test still ran with Hibernate quietly present on the classpath.
 */
@DisplayName("QueryAuditExtension runs without Hibernate on the classpath (issue #248)")
class NoHibernateOnClasspathTest {

  @Test
  @DisplayName("full extension lifecycle completes against a plain JDBC DataSource")
  void fullLifecycleCompletesWithoutHibernate() throws Exception {
    DataSource dataSource = newH2DataSource();
    Fixture.dataSource = dataSource;
    QueryAuditExtension extension = new QueryAuditExtension();
    MethodContext context = new MethodContext(Fixture.class, "auditedQuery");

    try {
      extension.beforeEach(context);
      extension.beforeTestExecution(context);
      try (Connection connection = Fixture.dataSource.getConnection();
          Statement statement = connection.createStatement()) {
        statement.execute("SELECT 1");
      }
      extension.afterTestExecution(context);

      assertThatCode(() -> extension.afterEach(context)).doesNotThrowAnyException();
    } finally {
      extension.afterAll(context);
    }

    assertThat(Fixture.dataSource).isSameAs(dataSource);
  }

  private static DataSource newH2DataSource() {
    JdbcDataSource dataSource = new JdbcDataSource();
    dataSource.setURL("jdbc:h2:mem:no_hibernate_extension;DB_CLOSE_DELAY=-1");
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
class Fixture {

  static DataSource dataSource;

  @ExpectQueries(select = 1)
  void auditedQuery() {}
}
