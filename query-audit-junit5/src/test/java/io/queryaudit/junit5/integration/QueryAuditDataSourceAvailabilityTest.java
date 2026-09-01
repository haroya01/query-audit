package io.queryaudit.junit5.integration;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.queryaudit.junit5.QueryAudit;
import io.queryaudit.junit5.QueryAuditExclude;
import io.queryaudit.junit5.QueryAuditExtension;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtensionConfigurationException;
import org.junit.jupiter.api.parallel.ResourceLock;
import org.junit.jupiter.api.parallel.Resources;

@DisplayName("QueryAudit DataSource availability (issue #197)")
@ResourceLock(Resources.SYSTEM_PROPERTIES)
class QueryAuditDataSourceAvailabilityTest {

  @AfterEach
  void clearAuditMode() {
    System.clearProperty("queryAudit.mode");
    System.clearProperty("queryGuard.mode");
  }

  @Test
  @DisplayName("an explicitly audited class fails when no DataSource can be resolved")
  void annotatedClassWithoutDataSourceFails() {
    assertThatThrownBy(
            () ->
                new QueryAuditExtension()
                    .beforeAll(new StubExtensionContext(AnnotatedFixture.class)))
        .isInstanceOf(ExtensionConfigurationException.class)
        .hasMessageContaining("DataSource unavailable")
        .hasMessageContaining(AnnotatedFixture.class.getName());
  }

  @Test
  @DisplayName("mode=all fails an included class when no DataSource can be resolved")
  void allModeClassWithoutDataSourceFails() {
    System.setProperty("queryAudit.mode", "all");

    assertThatThrownBy(
            () -> new QueryAuditExtension().beforeAll(new StubExtensionContext(PlainFixture.class)))
        .isInstanceOf(ExtensionConfigurationException.class)
        .hasMessageContaining("DataSource unavailable")
        .hasMessageContaining(PlainFixture.class.getName());
  }

  @Test
  @DisplayName("an inactive class without a DataSource remains outside the audit")
  void inactiveClassWithoutDataSourceIsIgnored() {
    assertThatCode(
            () -> new QueryAuditExtension().beforeAll(new StubExtensionContext(PlainFixture.class)))
        .doesNotThrowAnyException();
  }

  @Test
  @DisplayName("@QueryAuditExclude remains the mode=all escape hatch for non-database tests")
  void excludedClassWithoutDataSourceIsIgnoredInAllMode() {
    System.setProperty("queryAudit.mode", "all");

    assertThatCode(
            () ->
                new QueryAuditExtension()
                    .beforeAll(new StubExtensionContext(ExcludedFixture.class)))
        .doesNotThrowAnyException();
  }

  @QueryAudit
  static class AnnotatedFixture {}

  static class PlainFixture {}

  @QueryAuditExclude
  static class ExcludedFixture {}
}
