package io.queryaudit.junit5;

import io.queryaudit.core.detector.QueryAuditAnalyzer;
import io.queryaudit.core.detector.RepositoryReturnTypeResolver;
import io.queryaudit.core.parser.EnhancedSqlParser;
import io.queryaudit.core.provenance.AuditCapabilities;
import io.queryaudit.core.provenance.AuditCapability;
import io.queryaudit.core.provenance.AuditInputFingerprints;
import io.queryaudit.core.provenance.AuditPolicyInputs;
import io.queryaudit.core.provenance.AuditRuntimeIdentity;
import io.queryaudit.core.provenance.ComparisonInputs;
import java.util.Locale;

/** Immutable capability results shared by tests using the same initialized audit scope. */
record AuditInputContext(
    String dialect,
    AuditCapability indexMetadata,
    AuditCapability hibernateEvents,
    AuditCapability repositoryReturnTypes,
    RepositoryReturnTypeResolver initializedRepositoryResolver) {

  ComparisonInputs describe(
      QueryAuditAnalyzer analyzer, AuditPolicyInputs policy, AuditCapability explain) {
    RepositoryReturnTypeResolver effectiveResolver =
        analyzer.getConfig().getRepositoryReturnTypeResolver();
    AuditCapability returnTypes = repositoryReturnTypes;
    if (effectiveResolver != null && effectiveResolver != initializedRepositoryResolver) {
      returnTypes =
          AuditCapability.available(
              AuditRuntimeIdentity.unverifiedImplementation(effectiveResolver.getClass()), false);
    }
    return new ComparisonInputs(
        AuditRuntimeIdentity.queryAuditVersion(),
        analyzer.getConfig().getRuleProfile().name().toLowerCase(Locale.ROOT),
        dialect,
        EnhancedSqlParser.parserName(),
        EnhancedSqlParser.parserVersion(),
        analyzer.getRules().stream()
            .map(
                rule ->
                    analyzer.hasCompleteRuleInputs()
                        ? AuditRuntimeIdentity.implementation(rule.getClass())
                        : AuditRuntimeIdentity.unverifiedImplementation(rule.getClass()))
            .sorted()
            .toList(),
        analyzer.hasCompleteRuleInputs(),
        new AuditCapabilities(indexMetadata, hibernateEvents, explain, returnTypes),
        AuditInputFingerprints.create(analyzer.getConfig(), analyzer.getBaseline(), policy));
  }
}
