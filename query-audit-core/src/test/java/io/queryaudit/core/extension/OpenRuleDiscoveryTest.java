package io.queryaudit.core.extension;

import static io.queryaudit.core.extension.OpenFindingContractTest.*;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.queryaudit.core.config.QueryAuditConfig;
import io.queryaudit.core.detector.QueryAuditAnalyzer;
import io.queryaudit.core.model.Finding;
import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.Enumeration;
import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;

@Execution(ExecutionMode.SAME_THREAD)
class OpenRuleDiscoveryTest {
  @TempDir Path tempDir;

  @Test
  void newSpiDiscoveryIsDeterministicAndAvailableToLegacyAnalyzerConstructors() throws Exception {
    withProviders(
        List.of(ZProvider.class, AProvider.class),
        () -> {
          QueryAuditAnalyzer analyzer = new QueryAuditAnalyzer(config(), List.of());
          assertThat(analyzer.getAuditRules())
              .extracting(rule -> rule.getClass().getName())
              .containsExactly(AProvider.class.getName(), ZProvider.class.getName());
          assertThat(analyzer.hasCompleteRuleInputs()).isFalse();
          assertThat(
                  analyzer.analyze("discovery", List.of(QUERY), null).getCustomConfirmedFindings())
              .extracting(finding -> finding.kindId().value())
              .containsExactly("acme:a-kind", "acme:z-kind");
        });
  }

  @Test
  void sameImplementationDiscoveredAndExplicitIsRejectedOnlyWhenActive() throws Exception {
    withProviders(
        List.of(AProvider.class),
        () -> {
          AuditExtensions extensions =
              AuditExtensions.builder().auditRule("explicit", new AProvider()).build();
          assertThatThrownBy(
                  () ->
                      QueryAuditAnalyzer.withExtensions(
                          config(), tempDir.resolve("missing"), extensions))
              .isInstanceOf(IllegalArgumentException.class)
              .hasMessageContaining("both ServiceLoader and explicit");
          QueryAuditConfig disabled =
              QueryAuditConfig.builder()
                  .addDisabledRule("service-loader-detection-rule")
                  .addDisabledRule("acme:a-kind")
                  .build();
          QueryAuditAnalyzer analyzer =
              QueryAuditAnalyzer.withExtensions(disabled, tempDir.resolve("missing"), extensions);
          assertThat(analyzer.getAuditRules()).isEmpty();
          assertThat(analyzer.hasCompleteRuleInputs()).isTrue();
        });
  }

  @Test
  void duplicateRuleIdsAreRejectedAcrossDifferentImplementations() throws Exception {
    withProviders(
        List.of(AProvider.class),
        () -> {
          AuditRule explicit =
              rule(descriptor("acme:a-provider", Set.of(KIND)), ignored -> List.of());
          assertThatThrownBy(
                  () ->
                      QueryAuditAnalyzer.withExtensions(
                          config(),
                          tempDir.resolve("missing"),
                          AuditExtensions.builder().auditRule("explicit", explicit).build()))
              .isInstanceOf(IllegalArgumentException.class)
              .hasMessageContaining("Duplicate audit rule ID");
        });
  }

  @Test
  void findingKindOwnershipCannotSilentlyChangeBetweenTwoRules() {
    AuditRule first = rule(descriptor("acme:first", Set.of(KIND)), ignored -> List.of());
    AuditRule second = rule(descriptor("acme:second", Set.of(KIND)), ignored -> List.of());
    assertThatThrownBy(
            () ->
                QueryAuditAnalyzer.withExtensions(
                    config(),
                    tempDir.resolve("missing"),
                    AuditExtensions.builder()
                        .auditRule("first", first)
                        .auditRule("second", second)
                        .build()))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("declared by both");
  }

  @Test
  void explicitlyConfiguredInstancesOfOneClassCanHaveDifferentRuleAndKindIds() {
    AuditRule first = rule(descriptor("acme:first", Set.of(KIND)), ignored -> List.of());
    AuditRule second =
        rule(
            descriptor("acme:second", Set.of(FindingKindId.of("acme:second-kind"))),
            ignored -> List.of());
    QueryAuditAnalyzer analyzer =
        QueryAuditAnalyzer.withExtensions(
            config(),
            tempDir.resolve("missing"),
            AuditExtensions.builder()
                .auditRule("first", first)
                .auditRule("second", second)
                .build());
    assertThat(analyzer.getAuditRules()).containsExactly(first, second);
  }

  @Test
  void discoveryFailureIsNotSilentlyTreatedAsNoCustomRules() throws Exception {
    withProviders(
        List.of(ThrowingProvider.class),
        () -> {
          assertThatThrownBy(() -> new QueryAuditAnalyzer(config(), List.of()))
              .isInstanceOf(java.util.ServiceConfigurationError.class);
        });
  }

  public static final class AProvider implements AuditRule {
    public RuleDescriptor descriptor() {
      return OpenFindingContractTest.descriptor(
          "acme:a-provider", Set.of(FindingKindId.of("acme:a-kind")));
    }

    public List<Finding> evaluate(RuleContext context) {
      return List.of(finding(FindingKindId.of("acme:a-kind")));
    }
  }

  public static final class ZProvider implements AuditRule {
    public RuleDescriptor descriptor() {
      return OpenFindingContractTest.descriptor(
          "acme:z-provider", Set.of(FindingKindId.of("acme:z-kind")));
    }

    public List<Finding> evaluate(RuleContext context) {
      return List.of(finding(FindingKindId.of("acme:z-kind")));
    }
  }

  public static final class ThrowingProvider implements AuditRule {
    public ThrowingProvider() {
      throw new IllegalStateException("broken provider");
    }

    public RuleDescriptor descriptor() {
      throw new AssertionError();
    }

    public List<Finding> evaluate(RuleContext context) {
      throw new AssertionError();
    }
  }

  private static QueryAuditConfig config() {
    return QueryAuditConfig.builder().addDisabledRule("service-loader-detection-rule").build();
  }

  private void withProviders(List<Class<? extends AuditRule>> providers, Runnable action)
      throws Exception {
    String resource = "META-INF/services/" + AuditRule.class.getName();
    Path service = tempDir.resolve(resource);
    Files.createDirectories(service.getParent());
    Files.writeString(service, String.join("\n", providers.stream().map(Class::getName).toList()));
    ClassLoader original = Thread.currentThread().getContextClassLoader();
    try (URLClassLoader loader =
        new URLClassLoader(new URL[] {tempDir.toUri().toURL()}, original) {
          @Override
          public Enumeration<URL> getResources(String name) throws IOException {
            return resource.equals(name)
                ? Collections.enumeration(List.of(service.toUri().toURL()))
                : super.getResources(name);
          }
        }) {
      Thread.currentThread().setContextClassLoader(loader);
      try {
        action.run();
      } finally {
        Thread.currentThread().setContextClassLoader(original);
      }
    }
  }
}
