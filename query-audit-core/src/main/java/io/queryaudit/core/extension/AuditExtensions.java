package io.queryaudit.core.extension;

import io.queryaudit.core.analyzer.ExplainAnalyzer;
import io.queryaudit.core.analyzer.IndexMetadataProvider;
import io.queryaudit.core.detector.DetectionRule;
import io.queryaudit.core.reporter.delivery.AuditReportSink;
import io.queryaudit.core.reporter.delivery.ReportSinkRegistration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Explicit, framework-independent registrations for the existing QueryAudit extension interfaces.
 * Hosts can assemble the same catalog from application code or dependency-injection beans.
 *
 * <p>Registrations retain insertion order within each extension type. A built catalog is an
 * immutable snapshot of its builder; extension implementations themselves are not copied and must
 * be safe for the host's execution lifecycle. Registration IDs are unique across the whole catalog
 * and identify registrations only: they do not replace {@link DetectionRule#getRuleCode()} or
 * {@link FindingKindId}. Legacy rules retain the existing {@code IssueType}-backed model; open
 * {@link AuditRule} implementations declare namespaced finding kinds.
 *
 * <p>This catalog does not discover, execute, or grant trust to extensions. Hosts continue to apply
 * rule selection, severity overrides, suppression, evidence handling, and completeness checks.
 * Explicit registration does not describe an implementation's hidden configuration, so custom
 * extensions remain unverified comparison inputs unless the host can account for those inputs.
 * Legacy ServiceLoader discovery remains available independently of this catalog. When the analyzer
 * finds the same active rule implementation through both discovery and explicit registration, it
 * rejects the conflict rather than running the rule twice or choosing an instance.
 */
public final class AuditExtensions {
  private final Set<String> registrationIds;
  private final List<DetectionRule> rules;
  private final List<AuditRule> auditRules;
  private final Map<String, DetectionRule> rulesById;
  private final Map<String, AuditRule> auditRulesById;
  private final List<IndexMetadataProvider> indexMetadataProviders;
  private final Map<String, IndexMetadataProvider> indexMetadataProvidersById;
  private final List<ExplainAnalyzer> explainAnalyzers;
  private final Map<String, ExplainAnalyzer> explainAnalyzersById;
  private final List<ReportSinkRegistration> reportSinks;

  private AuditExtensions(Builder builder) {
    registrationIds = Collections.unmodifiableSet(new LinkedHashSet<>(builder.registrationIds));
    rulesById = Collections.unmodifiableMap(new LinkedHashMap<>(builder.rulesById));
    auditRulesById = Collections.unmodifiableMap(new LinkedHashMap<>(builder.auditRulesById));
    rules = List.copyOf(rulesById.values());
    auditRules = List.copyOf(auditRulesById.values());
    indexMetadataProviders = List.copyOf(builder.indexMetadataProvidersById.values());
    indexMetadataProvidersById =
        Collections.unmodifiableMap(new LinkedHashMap<>(builder.indexMetadataProvidersById));
    explainAnalyzers = List.copyOf(builder.explainAnalyzersById.values());
    explainAnalyzersById =
        Collections.unmodifiableMap(new LinkedHashMap<>(builder.explainAnalyzersById));
    reportSinks = List.copyOf(builder.reportSinks);
  }

  /** Returns an empty explicit catalog; legacy host discovery is not disabled. */
  public static AuditExtensions empty() {
    return builder().build();
  }

  public static Builder builder() {
    return new Builder();
  }

  /**
   * Catalog-wide registration IDs in insertion order, for assembly diagnostics, not rule policy.
   */
  public Set<String> registrationIds() {
    return registrationIds;
  }

  /** Explicit rules in registration order, without replacing built-in or discovered rules. */
  public List<DetectionRule> rules() {
    return rules;
  }

  /** Open rules in registration order, without replacing legacy or built-in rules. */
  public List<AuditRule> auditRules() {
    return auditRules;
  }

  /** Legacy rules keyed by assembly ID, retaining registration order and repeated instances. */
  public Map<String, DetectionRule> rulesById() {
    return rulesById;
  }

  /** Open rules keyed by assembly ID; this ID is distinct from the rule's declared identity. */
  public Map<String, AuditRule> auditRulesById() {
    return auditRulesById;
  }

  /** Explicit index metadata providers in registration order. */
  public List<IndexMetadataProvider> indexMetadataProviders() {
    return indexMetadataProviders;
  }

  /**
   * Explicit index metadata providers keyed by registration ID, in registration order. IDs identify
   * assembly, not trusted diagnostic text; hosts must sanitize them before including them in
   * output.
   */
  public Map<String, IndexMetadataProvider> indexMetadataProvidersById() {
    return indexMetadataProvidersById;
  }

  /** Explicit EXPLAIN analyzers in registration order. */
  public List<ExplainAnalyzer> explainAnalyzers() {
    return explainAnalyzers;
  }

  /** Explicit EXPLAIN providers keyed by assembly ID; hosts must sanitize IDs before output. */
  public Map<String, ExplainAnalyzer> explainAnalyzersById() {
    return explainAnalyzersById;
  }

  /** Report delivery registrations in insertion order; execution is controlled by the host. */
  public List<ReportSinkRegistration> reportSinks() {
    return reportSinks;
  }

  /** Mutable assembly helper; each call to {@link #build()} creates an independent snapshot. */
  public static final class Builder {
    private final Set<String> registrationIds = new LinkedHashSet<>();
    private final Map<String, DetectionRule> rulesById = new LinkedHashMap<>();
    private final Map<String, AuditRule> auditRulesById = new LinkedHashMap<>();
    private final Map<String, IndexMetadataProvider> indexMetadataProvidersById =
        new LinkedHashMap<>();
    private final Map<String, ExplainAnalyzer> explainAnalyzersById = new LinkedHashMap<>();
    private final List<ReportSinkRegistration> reportSinks = new ArrayList<>();

    private Builder() {}

    /** Adds a rule under a nonblank ID that is unique across all extension types. */
    public Builder rule(String id, DetectionRule rule) {
      Objects.requireNonNull(rule, "rule");
      registerId(id);
      rulesById.put(id, rule);
      return this;
    }

    /** Adds an open rule under a catalog-wide unique assembly ID, not a policy code. */
    public Builder auditRule(String id, AuditRule rule) {
      Objects.requireNonNull(rule, "auditRule");
      registerId(id);
      auditRulesById.put(id, rule);
      return this;
    }

    /** Adds an index metadata provider under a nonblank, catalog-wide unique ID. */
    public Builder indexMetadataProvider(String id, IndexMetadataProvider provider) {
      Objects.requireNonNull(provider, "indexMetadataProvider");
      registerId(id);
      indexMetadataProvidersById.put(id, provider);
      return this;
    }

    /** Adds an EXPLAIN analyzer under a nonblank, catalog-wide unique ID. */
    public Builder explainAnalyzer(String id, ExplainAnalyzer analyzer) {
      Objects.requireNonNull(analyzer, "explainAnalyzer");
      registerId(id);
      explainAnalyzersById.put(id, analyzer);
      return this;
    }

    /** Adds a host-delivered sink; required delivery failures affect the host verdict. */
    public Builder reportSink(String id, boolean required, AuditReportSink sink) {
      ReportSinkRegistration registration = new ReportSinkRegistration(id, required, sink);
      registerId(id);
      reportSinks.add(registration);
      return this;
    }

    public AuditExtensions build() {
      return new AuditExtensions(this);
    }

    private void registerId(String id) {
      if (id == null || id.isBlank()) {
        throw new IllegalArgumentException("Extension registration ID must not be blank");
      }
      if (!registrationIds.add(id)) {
        throw new IllegalArgumentException("Duplicate extension registration ID: " + id);
      }
    }
  }
}
