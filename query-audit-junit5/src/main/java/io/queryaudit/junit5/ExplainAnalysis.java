package io.queryaudit.junit5;

import io.queryaudit.core.analyzer.ExplainAnalysisException;
import io.queryaudit.core.analyzer.ExplainAnalyzer;
import io.queryaudit.core.config.QueryAuditConfig;
import io.queryaudit.core.detector.QueryAuditAnalyzer;
import io.queryaudit.core.extension.AuditExtensions;
import io.queryaudit.core.model.IncompleteReasonCode;
import io.queryaudit.core.model.IssueType;
import io.queryaudit.core.model.QueryAuditReport;
import io.queryaudit.core.model.QueryRecord;
import io.queryaudit.core.provenance.AuditCapability;
import io.queryaudit.core.provenance.AuditRuntimeIdentity;
import java.sql.Connection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.ServiceConfigurationError;
import java.util.ServiceLoader;
import java.util.function.Predicate;
import javax.sql.DataSource;

/**
 * Selects database plan providers and records capability failures without hiding partial evidence.
 */
final class ExplainAnalysis {
  QueryAuditReport analyze(
      AuditScope scope,
      AuditExtensions extensions,
      QueryAuditReport report,
      List<QueryRecord> queries,
      QueryAuditAnalyzer analyzer) {
    scope.explainCapability(AuditCapability.absent());
    DataSource dataSource = scope.dataSource();
    if (dataSource == null) {
      return report;
    }

    String source = "explain-discovery";
    DatabaseProviders.Selected<ExplainAnalyzer> selected = null;
    try {
      Predicate<ExplainAnalyzer> enabled =
          provider ->
              !AuditRuntimeIdentity.hasKnownCapabilityInputs(provider.getClass())
                  || hasEnabledExplainRules(analyzer.getConfig());
      Map<String, ExplainAnalyzer> explicitProviders = new LinkedHashMap<>();
      extensions
          .explainAnalyzersById()
          .forEach(
              (id, provider) -> {
                if (enabled.test(provider)) explicitProviders.put(id, provider);
              });
      Iterable<ExplainAnalyzer> providers;
      if (explicitProviders.isEmpty()) {
        List<ExplainAnalyzer> discovered = discoverExplainAnalyzers(enabled);
        if (discovered.isEmpty()) {
          return report;
        }
        providers = discovered;
      } else {
        // A matching explicit provider must not instantiate unrelated ServiceLoader providers.
        providers = () -> discoverExplainAnalyzers(enabled).iterator();
      }
      try (Connection connection = dataSource.getConnection()) {
        String dbProduct =
            connection.getMetaData().getDatabaseProductName().toLowerCase(Locale.ROOT);
        selected =
            DatabaseProviders.selectWithIdentity(
                dbProduct, explicitProviders, providers, ExplainAnalyzer::supportedDatabase);
        ExplainAnalyzer explainAnalyzer = selected == null ? null : selected.provider();
        if (explainAnalyzer != null) {
          source =
              AuditRuntimeIdentity.hasKnownCapabilityInputs(explainAnalyzer.getClass())
                  ? AuditRuntimeIdentity.implementation(explainAnalyzer.getClass())
                  : AuditRuntimeIdentity.unverifiedImplementation(explainAnalyzer.getClass());
          scope.explainCapability(
              AuditCapability.available(
                  source,
                  AuditRuntimeIdentity.hasKnownCapabilityInputs(explainAnalyzer.getClass())));
          if (!queries.isEmpty()) {
            report =
                analyzer.mergeDetectedIssues(
                    report,
                    Objects.requireNonNull(
                        explainAnalyzer.analyze(connection, queries),
                        "EXPLAIN providers must return a result list"));
          }
        }
      }
    } catch (Exception | LinkageError | ServiceConfigurationError failure) {
      AuditCapability capability = AuditCapability.failed(source);
      scope.explainCapability(capability);
      DatabaseProviders.Diagnostic diagnostic =
          failure instanceof DatabaseProviders.SelectionException selection
              ? selection.diagnostic()
              : selected == null ? null : selected.executionFailure();
      String detail =
          diagnostic == null
              ? "Capability failed: " + source
              : "EXPLAIN provider failed: " + diagnostic.description();
      if (failure instanceof ExplainAnalysisException incomplete) {
        // This final exception type exposes a host-owned reason message, never its raw cause.
        detail =
            "EXPLAIN failed ("
                + incomplete.getReason()
                + "): "
                + incomplete.getMessage()
                + (diagnostic == null ? "" : "; " + diagnostic.description());
        report = analyzer.mergeDetectedIssues(report, incomplete.getCompletedIssues());
      }
      AuditDiagnostics.incomplete(scope, IncompleteReasonCode.CAPABILITY_EXECUTION_FAILED, detail);
    }

    return report;
  }

  private static List<ExplainAnalyzer> discoverExplainAnalyzers(
      Predicate<ExplainAnalyzer> enabled) {
    return ServiceLoader.load(ExplainAnalyzer.class).stream()
        .map(ServiceLoader.Provider::get)
        .filter(enabled)
        .toList();
  }

  private static boolean hasEnabledExplainRules(QueryAuditConfig config) {
    return List.of(IssueType.FULL_TABLE_SCAN, IssueType.FILESORT, IssueType.TEMPORARY_TABLE)
        .stream()
        .anyMatch(type -> !config.isRuleExcluded(type.getCode()));
  }
}
