package io.queryaudit.junit5;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

import io.queryaudit.core.config.QueryAuditConfig;
import io.queryaudit.core.config.RuleProfile;
import java.lang.reflect.Method;
import java.util.Optional;
import java.util.Set;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.parallel.ResourceLock;
import org.junit.jupiter.api.parallel.Resources;

@ResourceLock(Resources.SYSTEM_PROPERTIES)
class AuditSettingsResolverTest {
  @Test
  void eachMethodResolvesFromTheBaseWithoutMutatingItOrReusingASiblingOverride() throws Exception {
    QueryAuditConfig base =
        QueryAuditConfig.builder()
            .nPlusOneThreshold(8)
            .failOnDetection(true)
            .suppressPatterns(Set.of("spring-rule"))
            .build();
    AuditSettingsResolver resolver = new AuditSettingsResolver(context -> base);

    QueryAuditConfig changed = resolver.buildConfig(context(PlainFixture.class, "changed"), null);
    QueryAuditConfig inherited =
        resolver.buildConfig(context(PlainFixture.class, "inherited"), null);

    assertThat(changed.getNPlusOneThreshold()).isEqualTo(5);
    assertThat(changed.isFailOnDetection()).isFalse();
    assertThat(changed.getSuppressPatterns())
        .containsExactlyInAnyOrder("spring-rule", "method-rule");
    assertThat(inherited.getNPlusOneThreshold()).isEqualTo(8);
    assertThat(inherited.isFailOnDetection()).isTrue();
    assertThat(inherited.getSuppressPatterns()).containsExactly("spring-rule");
    assertThat(base.getSuppressPatterns()).containsExactly("spring-rule");
  }

  @Test
  void methodAnnotationReplacesClassAnnotationWhileTheSpringBaseIsStillApplied() throws Exception {
    QueryAuditConfig base = QueryAuditConfig.builder().nPlusOneThreshold(8).build();
    AuditSettingsResolver resolver = new AuditSettingsResolver(context -> base);

    QueryAuditConfig config = resolver.buildConfig(context(ClassFixture.class, "method"), null);

    assertThat(config.getNPlusOneThreshold()).isEqualTo(8);
    assertThat(config.isIncludeSetupQueries()).isFalse();
    assertThat(config.getSuppressPatterns()).containsExactly("method-rule");
  }

  @Test
  void focusedAnnotationHasOneDiscoveryPathForSettingsAndAssertions() throws Exception {
    AuditSettingsResolver resolver = new AuditSettingsResolver(context -> null);
    ExtensionContext method = context(FocusedFixture.class, "method");

    assertThat(resolver.findDetectNPlusOne(method).threshold()).isEqualTo(11);
    assertThat(resolver.buildConfig(method, null).getNPlusOneThreshold()).isEqualTo(11);
    assertThat(resolver.findDetectNPlusOne(context(FocusedFixture.class, null)).threshold())
        .isEqualTo(7);
  }

  @Test
  void explicitSystemProfileOverridesTheSpringProfileWithoutFreezingTheNextResolution()
      throws Exception {
    String previous = System.getProperty("queryAudit.profile");
    try {
      QueryAuditConfig base = QueryAuditConfig.builder().ruleProfile(RuleProfile.MINIMAL).build();
      AuditSettingsResolver resolver = new AuditSettingsResolver(context -> base);
      System.setProperty("queryAudit.profile", "strict");
      assertThat(
              resolver.buildConfig(context(PlainFixture.class, "inherited"), null).getRuleProfile())
          .isEqualTo(RuleProfile.STRICT);
      System.clearProperty("queryAudit.profile");
      assertThat(
              resolver.buildConfig(context(PlainFixture.class, "inherited"), null).getRuleProfile())
          .isEqualTo(RuleProfile.MINIMAL);
    } finally {
      if (previous == null) System.clearProperty("queryAudit.profile");
      else System.setProperty("queryAudit.profile", previous);
    }
  }

  private static ExtensionContext context(Class<?> fixture, String methodName) throws Exception {
    ExtensionContext context = mock(ExtensionContext.class);
    doReturn(fixture).when(context).getRequiredTestClass();
    Method method = methodName == null ? null : fixture.getDeclaredMethod(methodName);
    when(context.getTestMethod()).thenReturn(Optional.ofNullable(method));
    return context;
  }

  static class PlainFixture {
    @QueryAudit(
        nPlusOneThreshold = 5,
        failOnDetection = BooleanOverride.FALSE,
        suppress = "method-rule")
    void changed() {}

    void inherited() {}
  }

  @QueryAudit(nPlusOneThreshold = 4, includeSetupQueries = true, suppress = "class-rule")
  static class ClassFixture {
    @QueryAudit(suppress = "method-rule")
    void method() {}
  }

  @DetectNPlusOne(threshold = 7)
  static class FocusedFixture {
    @QueryAudit(nPlusOneThreshold = 5)
    @DetectNPlusOne(threshold = 11)
    void method() {}
  }
}
