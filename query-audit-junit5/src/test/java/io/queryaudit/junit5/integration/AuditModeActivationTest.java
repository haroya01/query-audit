package io.queryaudit.junit5.integration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.queryaudit.junit5.EnableQueryInspector;
import io.queryaudit.junit5.QueryAudit;
import io.queryaudit.junit5.QueryAuditExclude;
import io.queryaudit.junit5.QueryAuditExtension;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Optional;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.ExtensionContext;

/**
 * Activation-decision tests for audit modes (issue #163): which test classes and methods the
 * extension audits under {@code annotated} (default) vs {@code all} mode, and how
 * {@code @QueryAuditExclude} interacts with both.
 */
@DisplayName("Issue #163: audit mode activation decision")
class AuditModeActivationTest {

  @AfterEach
  void clearModeProperty() {
    System.clearProperty("queryAudit.mode");
    System.clearProperty("queryGuard.mode");
  }

  // ── Fixture classes (never discovered as tests — no @Test methods) ──

  static class PlainFixture {
    @SuppressWarnings("unused")
    void probe() {}
  }

  @QueryAuditExclude
  static class ExcludedFixture {}

  @QueryAudit
  static class AnnotatedFixture {
    @SuppressWarnings("unused")
    void probe() {}

    @QueryAuditExclude
    @SuppressWarnings("unused")
    void excludedProbe() {}
  }

  @ExtendWith(QueryAuditExtension.class)
  static class DirectExtendWithFixture {}

  @EnableQueryInspector
  static class InspectorFixture {}

  @QueryAudit
  static class AnnotatedOuterFixture {
    class NestedFixture {}
  }

  // ── annotated mode (default) ──────────────────────────────────────

  @Nested
  @DisplayName("annotated mode (default)")
  class AnnotatedMode {

    @Test
    @DisplayName("a plain class is NOT audited — autodetection alone must not widen coverage")
    void plainClassInactive() throws Exception {
      assertThat(computeActive(PlainFixture.class)).isFalse();
    }

    @Test
    @DisplayName("@QueryAudit, @EnableQueryInspector, and direct @ExtendWith opt in")
    void optInsActivate() throws Exception {
      assertThat(computeActive(AnnotatedFixture.class)).isTrue();
      assertThat(computeActive(InspectorFixture.class)).isTrue();
      assertThat(computeActive(DirectExtendWithFixture.class)).isTrue();
    }

    @Test
    @DisplayName("a nested class inherits activation from its annotated enclosing class")
    void nestedInheritsFromEnclosing() throws Exception {
      assertThat(computeActive(AnnotatedOuterFixture.NestedFixture.class)).isTrue();
    }

    @Test
    @DisplayName("a method-level @QueryAuditExclude wins over a class-level @QueryAudit")
    void methodExcludeWinsOverClassAnnotation() throws Exception {
      assertThat(isAuditActive(AnnotatedFixture.class, "probe")).isTrue();
      assertThat(isAuditActive(AnnotatedFixture.class, "excludedProbe")).isFalse();
    }
  }

  // ── all mode ──────────────────────────────────────────────────────

  @Nested
  @DisplayName("all mode (queryAudit.mode=all)")
  class AllMode {

    @Test
    @DisplayName("a plain class IS audited")
    void plainClassActive() throws Exception {
      System.setProperty("queryAudit.mode", "all");
      assertThat(computeActive(PlainFixture.class)).isTrue();
    }

    @Test
    @DisplayName("@QueryAuditExclude on the class opts out")
    void excludedClassInactive() throws Exception {
      System.setProperty("queryAudit.mode", "all");
      assertThat(computeActive(ExcludedFixture.class)).isFalse();
    }

    @Test
    @DisplayName("the legacy queryGuard.mode property is honored")
    void legacyPropertyHonored() throws Exception {
      System.setProperty("queryGuard.mode", "all");
      assertThat(computeActive(PlainFixture.class)).isTrue();
    }

    @Test
    @DisplayName("an invalid mode value fails loudly instead of being silently ignored")
    void invalidModeThrows() {
      System.setProperty("queryAudit.mode", "banana");
      assertThatThrownBy(() -> computeActive(PlainFixture.class))
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining("banana");
    }
  }

  // ── Reflection plumbing ───────────────────────────────────────────

  private static boolean computeActive(Class<?> fixture) throws Exception {
    return invoke("computeActive", new StubExtensionContext(fixture));
  }

  private static boolean isAuditActive(Class<?> fixture, String methodName) throws Exception {
    return invoke("isAuditActive", new StubWithMethod(fixture, methodName));
  }

  private static boolean invoke(String name, ExtensionContext context) throws Exception {
    QueryAuditExtension extension = new QueryAuditExtension();
    Method method = QueryAuditExtension.class.getDeclaredMethod(name, ExtensionContext.class);
    method.setAccessible(true);
    try {
      return (boolean) method.invoke(extension, context);
    } catch (InvocationTargetException e) {
      throw (Exception) e.getCause();
    }
  }

  /** Stub variant that also exposes a test method, for method-level exclusion checks. */
  private static class StubWithMethod extends StubExtensionContext {

    private final Method method;

    StubWithMethod(Class<?> testClass, String methodName) throws NoSuchMethodException {
      super(testClass);
      this.method = testClass.getDeclaredMethod(methodName);
    }

    @Override
    public Optional<Method> getTestMethod() {
      return Optional.of(method);
    }
  }
}
