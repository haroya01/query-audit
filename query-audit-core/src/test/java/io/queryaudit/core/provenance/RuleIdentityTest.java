package io.queryaudit.core.provenance;

import static org.assertj.core.api.Assertions.assertThat;

import io.queryaudit.core.extension.FindingKindId;
import io.queryaudit.core.extension.RuleDescriptor;
import io.queryaudit.core.extension.RuleId;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.Test;

class RuleIdentityTest {
  @Test
  void hashesDeclaredSettingsVersionAndKindWithoutPublishingThem() {
    String original = identity("1", "shop:budget", Map.of("threshold", "secret-config-value"));
    assertThat(original)
        .startsWith("audit-rule:")
        .doesNotContain("secret-config-value", "shop:budget");
    assertThat(original)
        .isNotEqualTo(identity("2", "shop:budget", Map.of("threshold", "secret-config-value")));
    assertThat(original)
        .isNotEqualTo(identity("1", "shop:other", Map.of("threshold", "secret-config-value")));
    assertThat(original)
        .isNotEqualTo(identity("1", "shop:budget", Map.of("threshold", "different")));
  }

  @Test
  void mapOrderIsNotAnAnalysisChange() {
    Map<String, String> forward = new LinkedHashMap<>();
    forward.put("a", "1");
    forward.put("b", "2");
    Map<String, String> reverse = new LinkedHashMap<>();
    reverse.put("b", "2");
    reverse.put("a", "1");
    assertThat(identity("1", "shop:budget", forward))
        .isEqualTo(identity("1", "shop:budget", reverse));
  }

  @Test
  void unavailabilityHasOneTypedInterpretationAcrossConsumers() {
    assertThat(
            new ComparisonInputDifference("test", "capabilities.explain.state", "FAILED", "FAILED")
                .kind())
        .isEqualTo(ComparisonInputDifference.Kind.UNAVAILABLE);
    assertThat(
            new ComparisonInputDifference("test", "detectorInputsComplete", "true", "false").kind())
        .isEqualTo(ComparisonInputDifference.Kind.UNAVAILABLE);
    assertThat(new ComparisonInputDifference("test", "parser.version", "1", "2").kind())
        .isEqualTo(ComparisonInputDifference.Kind.CHANGED);
  }

  private static String identity(String version, String kind, Map<String, String> settings) {
    RuleDescriptor descriptor =
        new RuleDescriptor(
            new RuleId("shop:rule"), version, Set.of(FindingKindId.of(kind)), settings);
    return AuditRuntimeIdentity.ruleImplementation(RuleIdentityTest.class, descriptor);
  }
}
