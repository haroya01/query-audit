package io.queryaudit.core.config;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.queryaudit.core.detector.QueryAuditAnalyzer;
import io.queryaudit.core.model.IssueType;
import io.queryaudit.core.model.QueryAuditReport;
import io.queryaudit.core.model.QueryRecord;
import java.nio.file.Path;
import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

@DisplayName("RuleProfile (issue #164)")
class RuleProfileTest {

  @Nested
  @DisplayName("parse()")
  class Parse {

    @Test
    @DisplayName("all three tiers parse case-insensitively")
    void parsesTiers() {
      assertThat(RuleProfile.parse("strict")).isEqualTo(RuleProfile.STRICT);
      assertThat(RuleProfile.parse("RECOMMENDED")).isEqualTo(RuleProfile.RECOMMENDED);
      assertThat(RuleProfile.parse(" Minimal ")).isEqualTo(RuleProfile.MINIMAL);
    }

    @Test
    @DisplayName("null and blank use RECOMMENDED")
    void nullAndBlankDefaultToRecommended() {
      assertThat(RuleProfile.parse(null)).isEqualTo(RuleProfile.RECOMMENDED);
      assertThat(RuleProfile.parse(" ")).isEqualTo(RuleProfile.RECOMMENDED);
    }

    @Test
    @DisplayName("unknown values fail loudly, naming the accepted ones")
    void unknownValueThrows() {
      assertThatThrownBy(() -> RuleProfile.parse("paranoid"))
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining("paranoid")
          .hasMessageContaining("strict")
          .hasMessageContaining("recommended")
          .hasMessageContaining("minimal");
    }
  }

  @Nested
  @DisplayName("includes()")
  class Includes {

    @Test
    @DisplayName("STRICT runs everything, including opinionated rules")
    void strictIncludesEverything() {
      assertThat(RuleProfile.STRICT.includes("force-index-hint")).isTrue();
      assertThat(RuleProfile.STRICT.includes("n-plus-one")).isTrue();
    }

    @Test
    @DisplayName("RECOMMENDED drops the opinionated set but keeps high-precision rules")
    void recommendedDropsOpinionated() {
      assertThat(RuleProfile.RECOMMENDED.includes("force-index-hint")).isFalse();
      assertThat(RuleProfile.RECOMMENDED.includes("offset-pagination")).isFalse();
      assertThat(RuleProfile.RECOMMENDED.includes("like-leading-wildcard")).isFalse();
      assertThat(RuleProfile.RECOMMENDED.includes("n-plus-one")).isTrue();
      assertThat(RuleProfile.RECOMMENDED.includes("missing-where-index")).isTrue();
      assertThat(RuleProfile.RECOMMENDED.includes("update-without-where")).isTrue();
    }

    @Test
    @DisplayName("MINIMAL runs only the safety-critical allow-list")
    void minimalIsAllowListOnly() {
      assertThat(RuleProfile.MINIMAL.includes("n-plus-one")).isTrue();
      assertThat(RuleProfile.MINIMAL.includes("update-without-where")).isTrue();
      assertThat(RuleProfile.MINIMAL.includes("select-all")).isFalse();
      assertThat(RuleProfile.MINIMAL.includes("duplicate-query")).isFalse();
    }
  }

  @Nested
  @DisplayName("QueryAuditConfig.isRuleExcluded() precedence")
  class Precedence {

    @Test
    @DisplayName("disabled-rules wins over enabled-rules, which wins over the profile")
    void precedenceOrder() {
      QueryAuditConfig config =
          QueryAuditConfig.builder()
              .ruleProfile(RuleProfile.RECOMMENDED)
              .addEnabledRule("force-index-hint")
              .addEnabledRule("n-plus-one")
              .addDisabledRule("n-plus-one")
              .build();

      // explicitly disabled beats everything, even an explicit enable
      assertThat(config.isRuleExcluded("n-plus-one")).isTrue();
      // explicitly enabled beats the profile exclusion
      assertThat(config.isRuleExcluded("force-index-hint")).isFalse();
      // profile decides the rest
      assertThat(config.isRuleExcluded("offset-pagination")).isTrue();
      assertThat(config.isRuleExcluded("missing-where-index")).isFalse();
    }

    @Test
    @DisplayName("RECOMMENDED is the default in both config factories")
    void recommendedDefaultExcludesOpinionatedRules() {
      QueryAuditConfig config = QueryAuditConfig.defaults();
      assertThat(config.getRuleProfile()).isEqualTo(RuleProfile.RECOMMENDED);
      assertThat(QueryAuditConfig.builder().build().getRuleProfile())
          .isEqualTo(RuleProfile.RECOMMENDED);
      assertThat(config.isRuleExcluded("force-index-hint")).isTrue();
      assertThat(config.isRuleExcluded("n-plus-one")).isFalse();
    }

    @Test
    void nullProfileLeavesTheCurrentValueUnchanged() {
      assertThat(QueryAuditConfig.builder().ruleProfile(null).build().getRuleProfile())
          .isEqualTo(RuleProfile.RECOMMENDED);
      assertThat(QueryAuditConfig.builder().ruleProfile(RuleProfile.STRICT).ruleProfile(null)
              .build().getRuleProfile())
          .isEqualTo(RuleProfile.STRICT);
    }

    @Test
    @DisplayName("Builder.from() copies profile and enabled rules")
    void builderFromCopies() {
      QueryAuditConfig source =
          QueryAuditConfig.builder()
              .ruleProfile(RuleProfile.MINIMAL)
              .enabledRules(Set.of("select-all"))
              .build();
      QueryAuditConfig copy = QueryAuditConfig.Builder.from(source).build();
      assertThat(copy.getRuleProfile()).isEqualTo(RuleProfile.MINIMAL);
      assertThat(copy.getEnabledRules()).containsExactly("select-all");
    }
  }

  @Nested
  @DisplayName("End-to-end through QueryAuditAnalyzer")
  class ThroughAnalyzer {

    @TempDir Path tempDir;

    private QueryAuditReport analyze(QueryAuditConfig config, String sql) {
      QueryAuditAnalyzer analyzer =
          new QueryAuditAnalyzer(config, tempDir.resolve("baseline.json"));
      QueryRecord record = new QueryRecord(sql, 1_000L, 0L, "at com.example.T.m(T.java:1)");
      return analyzer.analyze("TC", "tm", List.of(record), null);
    }

    private static final String FORCE_INDEX_SQL =
        "SELECT id FROM orders FORCE INDEX (idx_user) WHERE user_id = 1";

    @Test
    @DisplayName("RECOMMENDED profile silences an opinionated detector")
    void recommendedSilencesOpinionatedDetector() {
      QueryAuditConfig strict = QueryAuditConfig.builder().ruleProfile(RuleProfile.STRICT).build();
      QueryAuditConfig recommended = QueryAuditConfig.defaults();

      assertThat(hasIssue(analyze(strict, FORCE_INDEX_SQL), IssueType.FORCE_INDEX_HINT))
          .as("strict fires force-index-hint")
          .isTrue();
      assertThat(hasIssue(analyze(recommended, FORCE_INDEX_SQL), IssueType.FORCE_INDEX_HINT))
          .as("recommended silences force-index-hint")
          .isFalse();
    }

    @Test
    @DisplayName("enabled-rules re-activates a rule the profile excludes")
    void enabledRulesReactivates() {
      QueryAuditConfig reEnabled =
          QueryAuditConfig.builder()
              .ruleProfile(RuleProfile.RECOMMENDED)
              .addEnabledRule("force-index-hint")
              .build();

      assertThat(hasIssue(analyze(reEnabled, FORCE_INDEX_SQL), IssueType.FORCE_INDEX_HINT))
          .isTrue();
    }

    private boolean hasIssue(QueryAuditReport report, IssueType type) {
      return java.util.stream.Stream.concat(
              report.getConfirmedIssues().stream(), report.getInfoIssues().stream())
          .anyMatch(issue -> issue.type() == type);
    }
  }
}
