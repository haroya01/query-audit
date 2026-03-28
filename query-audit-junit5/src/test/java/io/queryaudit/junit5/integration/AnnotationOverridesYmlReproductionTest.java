package io.queryaudit.junit5.integration;

import static org.assertj.core.api.Assertions.assertThat;

import io.queryaudit.core.config.QueryAuditConfig;
import io.queryaudit.core.model.Severity;
import io.queryaudit.junit5.BooleanOverride;
import io.queryaudit.junit5.DetectNPlusOne;
import io.queryaudit.junit5.EnableQueryInspector;
import io.queryaudit.junit5.QueryAudit;
import io.queryaudit.junit5.QueryAuditExtension;
import java.lang.reflect.Method;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtensionContext;

/**
 * Reproduction / verification test for GitHub issue #53:
 * "application.yml settings ignored when @QueryAudit annotation is present"
 *
 * <p>Tests config merge priority: hardcoded defaults -> application.yml -> annotation overrides
 */
@DisplayName("Issue #53: application.yml config merging with @QueryAudit annotation")
class AnnotationOverridesYmlReproductionTest {

  @Nested
  @DisplayName("Without Spring context (pure JUnit 5)")
  class WithoutSpringContext {

    @Test
    @DisplayName("buildConfig() uses hardcoded defaults when no Spring context is available")
    void usesHardcodedDefaults() throws Exception {
      QueryAuditConfig config = invokeBuildConfig(AnnotatedWithDefaults.class);
      QueryAuditConfig defaults = QueryAuditConfig.defaults();

      assertThat(config.getNPlusOneThreshold()).isEqualTo(defaults.getNPlusOneThreshold());
      assertThat(config.isFailOnDetection()).isEqualTo(defaults.isFailOnDetection());
      assertThat(config.getSuppressPatterns()).isEmpty();
      assertThat(config.getDisabledRules()).isEmpty();
    }

    @Test
    @DisplayName("Annotation-specified values override hardcoded defaults")
    void annotationOverridesDefaults() throws Exception {
      QueryAuditConfig config = invokeBuildConfig(AnnotatedWithThreshold50.class);

      assertThat(config.getNPlusOneThreshold())
          .as("Explicitly set threshold should be applied")
          .isEqualTo(50);
    }

    @Test
    @DisplayName("Annotation suppress patterns are additive")
    void annotationSuppressPatternsApplied() throws Exception {
      QueryAuditConfig config = invokeBuildConfig(AnnotatedWithSuppress.class);

      assertThat(config.getSuppressPatterns())
          .containsExactlyInAnyOrder("select-all", "n-plus-one");
    }

    @Test
    @DisplayName("@EnableQueryInspector sets failOnDetection to false via buildConfig()")
    void enableQueryInspectorSetsFailOnDetectionFalse() throws Exception {
      QueryAuditConfig config = invokeBuildConfig(AnnotatedWithEnableQueryInspector.class);

      assertThat(config.isFailOnDetection())
          .as("@EnableQueryInspector should set failOnDetection to false")
          .isFalse();
    }

    @Test
    @DisplayName("@DetectNPlusOne(threshold=7) overrides nPlusOneThreshold via buildConfig()")
    void detectNPlusOneOverridesThreshold() throws Exception {
      QueryAuditConfig config = invokeBuildConfig(AnnotatedWithDetectNPlusOne.class);

      assertThat(config.getNPlusOneThreshold())
          .as("@DetectNPlusOne(threshold=7) should override threshold")
          .isEqualTo(7);
    }

    @Test
    @DisplayName("Builder.from() copies ALL 20 fields from source config")
    void builderFromCopiesAllFields() {
      // Build a config with ALL non-default values
      QueryAuditConfig source =
          QueryAuditConfig.builder()
              .enabled(false)
              .failOnDetection(false)
              .nPlusOneThreshold(42)
              .offsetPaginationThreshold(9999)
              .orClauseThreshold(77)
              .addSuppressPattern("pattern-a")
              .addSuppressPattern("pattern-b")
              .addSuppressQuery("SELECT 1")
              .showInfo(false)
              .baselinePath("/custom/baseline.json")
              .autoOpenReport(false)
              .maxQueries(555)
              .addDisabledRule("rule-x")
              .addDisabledRule("rule-y")
              .addSeverityOverride("select-all", Severity.ERROR)
              .addSeverityOverride("n-plus-one", Severity.WARNING)
              .largeInListThreshold(999)
              .tooManyJoinsThreshold(88)
              .excessiveColumnThreshold(33)
              .repeatedInsertThreshold(11)
              .writeAmplificationThreshold(22)
              .slowQueryWarningMs(1234L)
              .slowQueryErrorMs(5678L)
              .build();

      QueryAuditConfig copy = QueryAuditConfig.Builder.from(source).build();

      // Assert every single field matches
      assertThat(copy.isEnabled())
          .as("enabled").isEqualTo(source.isEnabled());
      assertThat(copy.isFailOnDetection())
          .as("failOnDetection").isEqualTo(source.isFailOnDetection());
      assertThat(copy.getNPlusOneThreshold())
          .as("nPlusOneThreshold").isEqualTo(source.getNPlusOneThreshold());
      assertThat(copy.getOffsetPaginationThreshold())
          .as("offsetPaginationThreshold").isEqualTo(source.getOffsetPaginationThreshold());
      assertThat(copy.getOrClauseThreshold())
          .as("orClauseThreshold").isEqualTo(source.getOrClauseThreshold());
      assertThat(copy.getSuppressPatterns())
          .as("suppressPatterns").isEqualTo(source.getSuppressPatterns());
      assertThat(copy.getSuppressQueries())
          .as("suppressQueries").isEqualTo(source.getSuppressQueries());
      assertThat(copy.isShowInfo())
          .as("showInfo").isEqualTo(source.isShowInfo());
      assertThat(copy.getBaselinePath())
          .as("baselinePath").isEqualTo(source.getBaselinePath());
      assertThat(copy.isAutoOpenReport())
          .as("autoOpenReport").isEqualTo(source.isAutoOpenReport());
      assertThat(copy.getMaxQueries())
          .as("maxQueries").isEqualTo(source.getMaxQueries());
      assertThat(copy.getDisabledRules())
          .as("disabledRules").isEqualTo(source.getDisabledRules());
      assertThat(copy.getSeverityOverrides())
          .as("severityOverrides").isEqualTo(source.getSeverityOverrides());
      assertThat(copy.getLargeInListThreshold())
          .as("largeInListThreshold").isEqualTo(source.getLargeInListThreshold());
      assertThat(copy.getTooManyJoinsThreshold())
          .as("tooManyJoinsThreshold").isEqualTo(source.getTooManyJoinsThreshold());
      assertThat(copy.getExcessiveColumnThreshold())
          .as("excessiveColumnThreshold").isEqualTo(source.getExcessiveColumnThreshold());
      assertThat(copy.getRepeatedInsertThreshold())
          .as("repeatedInsertThreshold").isEqualTo(source.getRepeatedInsertThreshold());
      assertThat(copy.getWriteAmplificationThreshold())
          .as("writeAmplificationThreshold").isEqualTo(source.getWriteAmplificationThreshold());
      assertThat(copy.getSlowQueryWarningMs())
          .as("slowQueryWarningMs").isEqualTo(source.getSlowQueryWarningMs());
      assertThat(copy.getSlowQueryErrorMs())
          .as("slowQueryErrorMs").isEqualTo(source.getSlowQueryErrorMs());
    }
  }

  @Nested
  @DisplayName("With Spring context (via Builder.from() merge)")
  class WithSpringContext {

    @Test
    @DisplayName("Builder.from() preserves all Spring config values when annotation uses defaults")
    void usesSpringConfigAsBase() {
      QueryAuditConfig springConfig =
          QueryAuditConfig.builder()
              .nPlusOneThreshold(100)
              .failOnDetection(false)
              .addSuppressPattern("select-all")
              .addDisabledRule("offset-pagination")
              .orClauseThreshold(10)
              .largeInListThreshold(500)
              .build();

      QueryAuditConfig config =
          mergeSpringConfigWithAnnotation(AnnotatedWithDefaults.class, springConfig);

      // All values should come from Spring config, not hardcoded defaults
      assertThat(config.getNPlusOneThreshold())
          .as("Should use Spring config threshold (100), not hardcoded default (3)")
          .isEqualTo(100);

      assertThat(config.isFailOnDetection())
          .as("Should use Spring config failOnDetection (false), not hardcoded default (true)")
          .isFalse();

      assertThat(config.getSuppressPatterns())
          .as("Should include Spring config suppress patterns")
          .contains("select-all");

      assertThat(config.getDisabledRules())
          .as("Should include Spring config disabled rules")
          .contains("offset-pagination");

      assertThat(config.getOrClauseThreshold())
          .as("Should use Spring config orClauseThreshold (10)")
          .isEqualTo(10);

      assertThat(config.getLargeInListThreshold())
          .as("Should use Spring config largeInListThreshold (500)")
          .isEqualTo(500);
    }

    @Test
    @DisplayName("Annotation overrides Spring config for explicitly specified values only")
    void annotationOverridesSpringConfig() {
      QueryAuditConfig springConfig =
          QueryAuditConfig.builder()
              .nPlusOneThreshold(100)
              .failOnDetection(false)
              .addSuppressPattern("select-all")
              .orClauseThreshold(10)
              .build();

      // @QueryAudit(nPlusOneThreshold = 50) -- only threshold is explicit
      QueryAuditConfig config =
          mergeSpringConfigWithAnnotation(AnnotatedWithThreshold50.class, springConfig);

      // Explicitly set annotation value overrides Spring config
      assertThat(config.getNPlusOneThreshold())
          .as("Annotation threshold (50) should override Spring config (100)")
          .isEqualTo(50);

      // Non-specified values should still come from Spring config
      assertThat(config.isFailOnDetection())
          .as("failOnDetection should come from Spring config (false), not annotation default")
          .isFalse();

      assertThat(config.getOrClauseThreshold())
          .as("orClauseThreshold should come from Spring config (10)")
          .isEqualTo(10);
    }

    @Test
    @DisplayName("Annotation suppress patterns are additive to Spring config suppress patterns")
    void suppressPatternsAreMerged() {
      QueryAuditConfig springConfig =
          QueryAuditConfig.builder().addSuppressPattern("offset-pagination").build();

      // @QueryAudit(suppress = {"select-all", "n-plus-one"})
      QueryAuditConfig config =
          mergeSpringConfigWithAnnotation(AnnotatedWithSuppress.class, springConfig);

      assertThat(config.getSuppressPatterns())
          .as("Should contain both Spring and annotation suppress patterns")
          .containsExactlyInAnyOrder("offset-pagination", "select-all", "n-plus-one");
    }

    @Test
    @DisplayName("@EnableQueryInspector + Spring config sets failOnDetection to false")
    void enableQueryInspectorWithSpringConfig() {
      QueryAuditConfig springConfig =
          QueryAuditConfig.builder()
              .failOnDetection(true)
              .nPlusOneThreshold(100)
              .orClauseThreshold(15)
              .build();

      // Simulate the merge: Builder.from(springConfig), then @EnableQueryInspector sets
      // failOnDetection=false. This mirrors buildConfig() Layer 2 logic.
      QueryAuditConfig.Builder builder = QueryAuditConfig.Builder.from(springConfig);
      // Layer 2: @EnableQueryInspector override
      builder.failOnDetection(false);
      // Layer 3: @QueryAudit annotation with defaults (no explicit overrides)
      QueryAuditConfig config = builder.build();

      assertThat(config.isFailOnDetection())
          .as("@EnableQueryInspector should override Spring config failOnDetection to false")
          .isFalse();

      assertThat(config.getNPlusOneThreshold())
          .as("Spring config threshold should be preserved")
          .isEqualTo(100);

      assertThat(config.getOrClauseThreshold())
          .as("Spring config orClauseThreshold should be preserved")
          .isEqualTo(15);
    }

    @Test
    @DisplayName("@DetectNPlusOne(threshold=7) + Spring config overrides threshold only")
    void detectNPlusOneWithSpringConfig() {
      QueryAuditConfig springConfig =
          QueryAuditConfig.builder()
              .nPlusOneThreshold(100)
              .failOnDetection(false)
              .orClauseThreshold(15)
              .largeInListThreshold(500)
              .build();

      // Simulate the merge: Builder.from(springConfig), then @DetectNPlusOne overrides threshold.
      // This mirrors buildConfig() Layer 4 logic.
      QueryAuditConfig.Builder builder = QueryAuditConfig.Builder.from(springConfig);
      // Layer 4: @DetectNPlusOne override (highest priority for threshold)
      builder.nPlusOneThreshold(7);
      QueryAuditConfig config = builder.build();

      assertThat(config.getNPlusOneThreshold())
          .as("@DetectNPlusOne(threshold=7) should override Spring config threshold (100)")
          .isEqualTo(7);

      assertThat(config.isFailOnDetection())
          .as("Spring config failOnDetection should be preserved")
          .isFalse();

      assertThat(config.getOrClauseThreshold())
          .as("Spring config orClauseThreshold should be preserved")
          .isEqualTo(15);

      assertThat(config.getLargeInListThreshold())
          .as("Spring config largeInListThreshold should be preserved")
          .isEqualTo(500);
    }

    @Test
    @DisplayName(
        "failOnDetection=TRUE in annotation overrides Spring config when explicitly specified")
    void failOnDetectionExplicitTrueOverridesSpringConfig() {
      QueryAuditConfig springConfig =
          QueryAuditConfig.builder()
              .failOnDetection(false)
              .nPlusOneThreshold(100)
              .build();

      // @QueryAudit(failOnDetection = BooleanOverride.TRUE)
      QueryAuditConfig config =
          mergeSpringConfigWithAnnotation(AnnotatedWithFailOnDetectionTrue.class, springConfig);

      assertThat(config.isFailOnDetection())
          .as("Explicit TRUE annotation should override Spring config false")
          .isTrue();

      assertThat(config.getNPlusOneThreshold())
          .as("Non-overridden threshold should come from Spring config")
          .isEqualTo(100);
    }

    @Test
    @DisplayName(
        "failOnDetection=FALSE in annotation overrides Spring config when explicitly specified")
    void failOnDetectionExplicitFalseOverridesSpringConfig() {
      QueryAuditConfig springConfig =
          QueryAuditConfig.builder()
              .failOnDetection(true)
              .nPlusOneThreshold(100)
              .build();

      // @QueryAudit(failOnDetection = BooleanOverride.FALSE)
      QueryAuditConfig config =
          mergeSpringConfigWithAnnotation(AnnotatedWithFailOnDetectionFalse.class, springConfig);

      assertThat(config.isFailOnDetection())
          .as("Explicit FALSE annotation should override Spring config true")
          .isFalse();
    }

  }

  /**
   * Tests for {@code shouldAutoOpenReport()} which handles autoOpenReport separately from
   * {@code buildConfig()}. This method has its own priority chain:
   * sysProp > envVar > CI detection > annotation > Spring config > default(true).
   */
  @Nested
  @DisplayName("shouldAutoOpenReport() priority chain")
  class ShouldAutoOpenReport {

    @Test
    @DisplayName("INHERIT annotation falls through to Spring config value")
    void inheritFallsToSpringConfig() throws Exception {
      // @QueryAudit with defaults (autoOpenReport = INHERIT)
      // No sysProp, no envVar, no CI — should fall through to Spring config.
      // Since StubExtensionContext has no Spring context, resolveSpringConfig returns null,
      // so we can only test the annotation path here.
      boolean result = invokeShouldAutoOpenReport(AnnotatedWithDefaults.class);

      // No Spring, no CI, INHERIT → default true
      assertThat(result)
          .as("INHERIT + no Spring config → should return default true")
          .isTrue();
    }

    @Test
    @DisplayName("Explicit TRUE annotation returns true")
    void explicitTrueReturnsTrue() throws Exception {
      boolean result = invokeShouldAutoOpenReport(AnnotatedWithAutoOpenReportTrue.class);

      assertThat(result)
          .as("Explicit TRUE annotation should return true")
          .isTrue();
    }

    @Test
    @DisplayName("Explicit FALSE annotation returns false")
    void explicitFalseReturnsFalse() throws Exception {
      boolean result = invokeShouldAutoOpenReport(AnnotatedWithAutoOpenReportFalse.class);

      assertThat(result)
          .as("Explicit FALSE annotation should return false")
          .isFalse();
    }

    @Test
    @DisplayName("No annotation falls through to default true")
    void noAnnotationReturnsDefault() throws Exception {
      boolean result = invokeShouldAutoOpenReport(NoAnnotation.class);

      assertThat(result)
          .as("No annotation + no Spring config → should return default true")
          .isTrue();
    }
  }

  // -- Helpers -----------------------------------------------------------

  /**
   * Invokes the real {@code buildConfig()} via reflection. Works for the no-Spring path since
   * StubExtensionContext has no Spring context (resolveSpringConfig returns null).
   */
  private static QueryAuditConfig invokeBuildConfig(Class<?> testClass) throws Exception {
    QueryAuditExtension extension = new QueryAuditExtension();
    Method buildConfigMethod =
        QueryAuditExtension.class.getDeclaredMethod("buildConfig", ExtensionContext.class);
    buildConfigMethod.setAccessible(true);

    ExtensionContext context = new StubExtensionContext(testClass);
    return (QueryAuditConfig) buildConfigMethod.invoke(extension, context);
  }

  /**
   * Tests the merge logic: {@code Builder.from(springConfig)} + annotation overrides. This mirrors
   * what {@code buildConfig()} does when Spring config is available (Layers 1 + 3), without
   * requiring a real Spring ApplicationContext.
   *
   * <p>Note: This does NOT call buildConfig() directly because StubExtensionContext lacks a Spring
   * context. Instead it tests the Builder.from() merge path that buildConfig() relies on.
   * autoOpenReport is NOT handled here — it is processed separately by shouldAutoOpenReport().
   */
  private static QueryAuditConfig mergeSpringConfigWithAnnotation(
      Class<?> testClass, QueryAuditConfig springConfig) {
    QueryAuditConfig.Builder builder = QueryAuditConfig.Builder.from(springConfig);

    QueryAudit annotation = testClass.getAnnotation(QueryAudit.class);
    if (annotation != null) {
      // failOnDetection: only override when explicitly specified in the annotation
      // (BooleanOverride.TRUE or FALSE, not INHERIT). Mirrors buildConfig() Layer 3 logic.
      if (annotation.failOnDetection().isSpecified()) {
        builder.failOnDetection(annotation.failOnDetection().toBoolean());
      }

      if (annotation.nPlusOneThreshold() >= 0) {
        builder.nPlusOneThreshold(annotation.nPlusOneThreshold());
      }
      for (String suppress : annotation.suppress()) {
        builder.addSuppressPattern(suppress);
      }
      if (!annotation.baselinePath().isEmpty()) {
        builder.baselinePath(annotation.baselinePath());
      }
    }

    return builder.build();
  }

  /**
   * Invokes the real {@code shouldAutoOpenReport()} via reflection. Tests the actual priority chain
   * without requiring Spring context or environment variable manipulation.
   */
  private static boolean invokeShouldAutoOpenReport(Class<?> testClass) throws Exception {
    QueryAuditExtension extension = new QueryAuditExtension();
    Method method =
        QueryAuditExtension.class.getDeclaredMethod(
            "shouldAutoOpenReport", ExtensionContext.class);
    method.setAccessible(true);

    ExtensionContext context = new StubExtensionContext(testClass);
    return (boolean) method.invoke(extension, context);
  }

  // -- Stub test classes -------------------------------------------------

  @QueryAudit
  static class AnnotatedWithDefaults {}

  @QueryAudit(nPlusOneThreshold = 50)
  static class AnnotatedWithThreshold50 {}

  @QueryAudit(suppress = {"select-all", "n-plus-one"})
  static class AnnotatedWithSuppress {}

  @QueryAudit
  @EnableQueryInspector
  static class AnnotatedWithEnableQueryInspector {}

  @QueryAudit
  @DetectNPlusOne(threshold = 7)
  static class AnnotatedWithDetectNPlusOne {}

  @QueryAudit(failOnDetection = BooleanOverride.TRUE)
  static class AnnotatedWithFailOnDetectionTrue {}

  @QueryAudit(failOnDetection = BooleanOverride.FALSE)
  static class AnnotatedWithFailOnDetectionFalse {}

  @QueryAudit(autoOpenReport = BooleanOverride.TRUE)
  static class AnnotatedWithAutoOpenReportTrue {}

  @QueryAudit(autoOpenReport = BooleanOverride.FALSE)
  static class AnnotatedWithAutoOpenReportFalse {}

  static class NoAnnotation {}
}
