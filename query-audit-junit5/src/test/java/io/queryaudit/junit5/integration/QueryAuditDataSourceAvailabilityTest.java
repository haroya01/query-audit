package io.queryaudit.junit5.integration;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.queryaudit.core.config.QueryAuditConfig;
import io.queryaudit.junit5.QueryAudit;
import io.queryaudit.junit5.QueryAuditExclude;
import io.queryaudit.junit5.QueryAuditExtension;
import java.lang.reflect.Method;
import java.util.Optional;
import javax.sql.DataSource;
import net.ttddyy.dsproxy.support.ProxyDataSource;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.ExtensionConfigurationException;
import org.junit.jupiter.api.parallel.ResourceLock;
import org.junit.jupiter.api.parallel.Resources;
import org.mockito.Mockito;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;

@DisplayName("QueryAudit DataSource availability (issue #197)")
@ResourceLock(Resources.SYSTEM_PROPERTIES)
class QueryAuditDataSourceAvailabilityTest {

  @AfterEach
  void clearAuditMode() {
    System.clearProperty("queryAudit.mode");
    System.clearProperty("queryGuard.mode");
  }

  @Test
  @DisplayName("an explicitly audited class fails when its active method has no DataSource")
  void annotatedClassWithoutDataSourceFails() throws Exception {
    StubWithMethod context = new StubWithMethod(AnnotatedFixture.class, "audited");

    assertThatThrownBy(() -> new QueryAuditExtension().beforeEach(context))
        .isInstanceOf(ExtensionConfigurationException.class)
        .hasMessageContaining("DataSource unavailable")
        .hasMessageContaining(AnnotatedFixture.class.getName() + "#audited");
  }

  @Test
  @DisplayName("mode=all fails an included method when no DataSource can be resolved")
  void allModeClassWithoutDataSourceFails() throws Exception {
    System.setProperty("queryAudit.mode", "all");
    StubWithMethod context = new StubWithMethod(PlainFixture.class, "audited");

    assertThatThrownBy(() -> new QueryAuditExtension().beforeEach(context))
        .isInstanceOf(ExtensionConfigurationException.class)
        .hasMessageContaining("DataSource unavailable")
        .hasMessageContaining(PlainFixture.class.getName() + "#audited");
  }

  @Test
  @DisplayName("method-level @QueryAudit also fails when no DataSource can be resolved")
  void methodAnnotationWithoutDataSourceFails() throws Exception {
    StubWithMethod context = new StubWithMethod(MethodAnnotatedFixture.class, "audited");

    assertThatThrownBy(() -> new QueryAuditExtension().beforeEach(context))
        .isInstanceOf(ExtensionConfigurationException.class)
        .hasMessageContaining("DataSource unavailable")
        .hasMessageContaining(MethodAnnotatedFixture.class.getName() + "#audited");
  }

  @Test
  @DisplayName("an inactive method without a DataSource remains outside the audit")
  void inactiveClassWithoutDataSourceIsIgnored() throws Exception {
    StubWithMethod context = new StubWithMethod(PlainFixture.class, "audited");

    assertThatCode(() -> new QueryAuditExtension().beforeEach(context)).doesNotThrowAnyException();
  }

  @Test
  @DisplayName("class-level @QueryAuditExclude remains the mode=all escape hatch")
  void excludedClassWithoutDataSourceIsIgnoredInAllMode() throws Exception {
    System.setProperty("queryAudit.mode", "all");
    StubWithMethod context = new StubWithMethod(ExcludedFixture.class, "audited");

    assertThatCode(() -> new QueryAuditExtension().beforeEach(context)).doesNotThrowAnyException();
  }

  @Test
  @DisplayName("method-level @QueryAuditExclude is checked before DataSource availability")
  void excludedMethodWithoutDataSourceIsIgnoredInAllMode() throws Exception {
    System.setProperty("queryAudit.mode", "all");
    QueryAuditExtension extension = new QueryAuditExtension();

    assertThatCode(() -> extension.beforeAll(new StubExtensionContext(PlainFixture.class)))
        .doesNotThrowAnyException();
    assertThatCode(() -> extension.beforeEach(new StubWithMethod(PlainFixture.class, "excluded")))
        .doesNotThrowAnyException();
  }

  @Test
  @DisplayName("a static DataSource keeps the active audit path operational")
  void staticDataSourceAllowsActiveAudit() throws Exception {
    QueryAuditExtension extension = new QueryAuditExtension();
    StubWithMethod context = new StubWithMethod(StaticDataSourceFixture.class, "audited");

    assertThatCode(
            () -> {
              extension.beforeEach(context);
              extension.afterEach(context);
            })
        .doesNotThrowAnyException();
  }

  @Nested
  @SpringJUnitConfig(DisabledConfig.class)
  @QueryAudit
  @DisplayName("when QueryAudit is disabled")
  class DisabledAudit {

    @Test
    @DisplayName("an annotated test does not require a DataSource")
    void disabledAuditDoesNotRequireDataSource() {}
  }

  @Nested
  @ExtendWith(QueryAuditExtension.class)
  @DisplayName("when an audited method is excluded")
  class ExcludedMethodAudit {

    @Test
    @QueryAuditExclude
    @DisplayName("the exclusion is applied before DataSource validation")
    void excludedAuditDoesNotRequireDataSource() {}
  }

  @Nested
  @QueryAudit
  @DisplayName("when a DataSource is assigned in user lifecycle setup")
  class LateDataSourceAudit {

    static DataSource dataSource;

    @BeforeAll
    static void assignDataSource() {
      dataSource = new ProxyDataSource(Mockito.mock(DataSource.class));
    }

    @AfterAll
    static void clearDataSource() {
      dataSource = null;
    }

    @Test
    @DisplayName("the active audit initializes after @BeforeAll")
    void dataSourceFromBeforeAllIsAccepted() {}
  }

  @Configuration(proxyBeanMethods = false)
  static class DisabledConfig {

    @Bean
    QueryAuditConfig queryAuditConfig() {
      return QueryAuditConfig.builder().enabled(false).build();
    }
  }

  @QueryAudit
  static class AnnotatedFixture {
    void audited() {}
  }

  static class PlainFixture {
    void audited() {}

    @QueryAuditExclude
    void excluded() {}
  }

  static class MethodAnnotatedFixture {
    @QueryAudit
    void audited() {}
  }

  @QueryAuditExclude
  static class ExcludedFixture {
    void audited() {}
  }

  @QueryAudit
  static class StaticDataSourceFixture {
    static final DataSource DATA_SOURCE = new ProxyDataSource(Mockito.mock(DataSource.class));

    void audited() {}
  }

  private static class StubWithMethod extends StubExtensionContext {

    private final Method method;

    StubWithMethod(Class<?> testClass, String methodName) throws NoSuchMethodException {
      super(testClass);
      method = testClass.getDeclaredMethod(methodName);
    }

    @Override
    public Optional<Method> getTestMethod() {
      return Optional.of(method);
    }
  }
}
