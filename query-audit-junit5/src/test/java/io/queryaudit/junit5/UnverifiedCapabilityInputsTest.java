package io.queryaudit.junit5;

import static org.assertj.core.api.Assertions.assertThat;

import io.queryaudit.core.analyzer.IndexMetadataProvider;
import io.queryaudit.core.config.QueryAuditConfig;
import io.queryaudit.core.detector.QueryAuditAnalyzer;
import io.queryaudit.core.detector.RepositoryReturnType;
import io.queryaudit.core.model.AuditOutcome;
import io.queryaudit.core.model.AuditRunResult;
import io.queryaudit.core.model.IndexMetadata;
import io.queryaudit.core.model.IssueType;
import io.queryaudit.core.model.QueryRecord;
import io.queryaudit.core.provenance.AuditCapability;
import io.queryaudit.core.provenance.AuditPolicyInputs;
import io.queryaudit.core.provenance.ComparisonInputCompatibility;
import io.queryaudit.core.provenance.ComparisonInputDifference;
import io.queryaudit.core.provenance.ComparisonInputs;
import java.sql.Connection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.h2.jdbcx.JdbcDataSource;
import org.junit.jupiter.api.Test;

class UnverifiedCapabilityInputsTest {
  private static final List<QueryRecord> QUERIES =
      List.of(
          new QueryRecord(
              "SELECT name FROM users WHERE status = ?", 1, 1, "example.Repository.load:12"));
  private static final AuditCapability ABSENT = AuditCapability.absent();

  @Test
  void lambdaResolverChangesCannotLookLikeAVerifiedFindingResolution() {
    QueryAuditAnalyzer before =
        new QueryAuditAnalyzer(
            QueryAuditConfig.builder()
                .repositoryReturnTypeResolver(stack -> RepositoryReturnType.UNKNOWN)
                .build(),
            List.of());
    QueryAuditAnalyzer after =
        new QueryAuditAnalyzer(
            QueryAuditConfig.builder()
                .repositoryReturnTypeResolver(stack -> RepositoryReturnType.SINGLE_ENTITY)
                .build(),
            List.of());
    assertThat(findings(before, null)).isEqualTo(1);
    assertThat(findings(after, null)).isZero();

    AuditInputContext context = new AuditInputContext("h2", ABSENT, ABSENT, ABSENT, null);
    ComparisonInputs previous = context.describe(before, AuditPolicyInputs.empty(), ABSENT);
    ComparisonInputs current = context.describe(after, AuditPolicyInputs.empty(), ABSENT);
    assertThat(ComparisonInputCompatibility.compare("test", previous, current))
        .extracting(ComparisonInputDifference::field)
        .contains("capabilities.repositoryReturnTypes.inputsComplete");
    assertStandalonePolicyIsUnchanged(current);
  }

  @Test
  void customMetadataProviderSettingsCannotLookLikeAVerifiedSchemaFix() {
    JdbcDataSource dataSource = new JdbcDataSource();
    dataSource.setURL("jdbc:h2:mem:custom-metadata-inputs");
    IndexMetadataCollector.Result before =
        new IndexMetadataCollector(List.of(new ConfiguredProvider(false)))
            .collectWithCapabilities(dataSource);
    IndexMetadataCollector.Result after =
        new IndexMetadataCollector(List.of(new ConfiguredProvider(true)))
            .collectWithCapabilities(dataSource);
    QueryAuditAnalyzer analyzer = new QueryAuditAnalyzer(QueryAuditConfig.defaults(), List.of());
    assertThat(findings(analyzer, before.metadata())).isEqualTo(1);
    assertThat(findings(analyzer, after.metadata())).isZero();
    ComparisonInputs previous =
        new AuditInputContext("h2", before.capability(), ABSENT, ABSENT, null)
            .describe(analyzer, AuditPolicyInputs.empty(), ABSENT);
    ComparisonInputs current =
        new AuditInputContext("h2", after.capability(), ABSENT, ABSENT, null)
            .describe(analyzer, AuditPolicyInputs.empty(), ABSENT);
    assertThat(ComparisonInputCompatibility.compare("test", previous, current))
        .extracting(ComparisonInputDifference::field)
        .contains("capabilities.indexMetadata.inputsComplete");
    assertStandalonePolicyIsUnchanged(current);
  }

  private static void assertStandalonePolicyIsUnchanged(ComparisonInputs inputs) {
    assertThat(
            AuditRunResult.pass(List.of()).withComparisonInputs(Map.of("test", inputs)).outcome())
        .isEqualTo(AuditOutcome.PASS);
  }

  private static long findings(QueryAuditAnalyzer analyzer, IndexMetadata metadata) {
    return analyzer.analyze("test", QUERIES, metadata).getConfirmedIssues().stream()
        .filter(issue -> issue.type() == IssueType.UNBOUNDED_RESULT_SET)
        .count();
  }

  private static final class ConfiguredProvider implements IndexMetadataProvider {
    private final boolean unique;

    ConfiguredProvider(boolean unique) {
      this.unique = unique;
    }

    public String supportedDatabase() {
      return "h2";
    }

    public IndexMetadata getIndexMetadata(Connection connection) {
      return new IndexMetadata(Map.of()) {
        @Override
        public boolean hasUniqueIndexCoveredBy(String table, Set<String> columns) {
          return unique;
        }
      };
    }
  }
}
