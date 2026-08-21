package io.queryaudit.core.config;

import java.util.Locale;
import java.util.Set;

/**
 * Named rule tiers controlling which detection rules run by default.
 *
 * <ul>
 *   <li>{@link #STRICT} — every rule (the pre-0.5.0 behavior, and the default).
 *   <li>{@link #RECOMMENDED} — everything except the opinionated / context-dependent rules that
 *       legitimately fire on correct SQL. Built for a quiet, trustworthy first run.
 *   <li>{@link #MINIMAL} — safety-critical rules only, for a lean CI gate.
 * </ul>
 *
 * <p>{@code RECOMMENDED} is deny-list based: a new rule joins it automatically unless it is added
 * to the opinionated set. Explicit {@code disabled-rules} / {@code enabled-rules} configuration
 * always wins over the profile. External rules registered via {@code ServiceLoader} without a rule
 * code are never filtered by profiles.
 *
 * <p>The tier assignment below is v1, derived from rule severity and the false-positive history in
 * the issue tracker; it is expected to be revised as per-rule hit/FP statistics accumulate.
 *
 * @author haroya
 * @since 0.5.0
 */
public enum RuleProfile {
  STRICT,
  RECOMMENDED,
  MINIMAL;

  /**
   * Rules excluded from {@link #RECOMMENDED}: style opinions, context-dependent judgments, and
   * report-only advisories that fire on legitimate SQL often enough to erode trust on first run.
   */
  private static final Set<String> OPINIONATED =
      Set.of(
          "force-index-hint", // hints are sometimes the right call
          "offset-pagination", // fine at small scale
          "like-leading-wildcard", // leading wildcards are often intentional
          "or-abuse", // style opinion
          "case-in-where", // style opinion
          "regexp-usage", // style opinion
          "find-in-set", // style opinion
          "having-misuse", // heuristic
          "distinct-misuse", // heuristic
          "union-without-all", // deduplication is often wanted
          "count-star-no-where", // legitimate table statistics
          "count-instead-of-exists", // cannot tell aggregates from existence checks (issue #126)
          "full-scan", // EXPLAIN advisory, environment-dependent
          "filesort", // EXPLAIN advisory, environment-dependent
          "temporary-table", // EXPLAIN advisory, environment-dependent
          "covering-index-opportunity", // advice, not a defect
          "n-plus-one-suspect", // SQL-level heuristic; the confirmed rule stays
          "mergeable-queries", // advice, not a defect
          "for-update-no-timeout", // environment-dependent
          "window-no-partition", // often deliberate over the full result
          "connection-held-idle" // wall-clock heuristic; needs realistic latency in tests
          );

  /**
   * The {@link #MINIMAL} allow-list: rules whose findings are near-certain production incidents.
   */
  private static final Set<String> SAFETY_CRITICAL =
      Set.of(
          "n-plus-one",
          "missing-where-index",
          "missing-join-index",
          "cartesian-join",
          "update-without-where",
          "unbounded-result-set",
          "slow-query");

  /** Returns whether this profile runs the rule with the given issue code. */
  public boolean includes(String issueCode) {
    return switch (this) {
      case STRICT -> true;
      case RECOMMENDED -> !OPINIONATED.contains(issueCode);
      case MINIMAL -> SAFETY_CRITICAL.contains(issueCode);
    };
  }

  /**
   * Parses a configuration value. Case-insensitive; {@code null} and blank map to {@link #STRICT}
   * so absent configuration keeps the pre-0.5.0 behavior.
   *
   * @throws IllegalArgumentException on any other value, naming the accepted ones
   */
  public static RuleProfile parse(String value) {
    if (value == null || value.isBlank()) {
      return STRICT;
    }
    return switch (value.trim().toLowerCase(Locale.ROOT)) {
      case "strict" -> STRICT;
      case "recommended" -> RECOMMENDED;
      case "minimal" -> MINIMAL;
      default ->
          throw new IllegalArgumentException(
              "Unknown query-audit profile '"
                  + value
                  + "' — expected 'strict', 'recommended', or 'minimal'");
    };
  }
}
