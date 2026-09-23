package io.queryaudit.core.config;

import io.queryaudit.core.model.IssueType;
import java.util.Arrays;
import java.util.Locale;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Named rule tiers controlling which detection rules run by default.
 *
 * <ul>
 *   <li>{@link #STRICT} — every rule (the default before 0.6.0).
 *   <li>{@link #RECOMMENDED} — the call-site N+1 rule only. The default since 0.7.0.
 *   <li>{@link #MINIMAL} — N+1 plus index, join, and write-safety rules.
 * </ul>
 *
 * <p>Both {@code RECOMMENDED} and {@code MINIMAL} are allow-lists over built-in rules: a new built-in
 * rule stays off until it is added to one explicitly. {@code RECOMMENDED} never filters custom
 * finding kinds. Explicit {@code disabled-rules} / {@code enabled-rules} configuration
 * always wins over the profile. External rules registered via {@code ServiceLoader} without a rule
 * code are never filtered by profiles.
 *
 * @author haroya
 * @since 0.5.0
 */
public enum RuleProfile {
  STRICT,
  RECOMMENDED,
  MINIMAL;

  private static final Set<String> RECOMMENDED_RULES = Set.of("n-plus-one");

  private static final Set<String> BUILT_IN_CODES =
      Arrays.stream(IssueType.values()).map(IssueType::getCode).collect(Collectors.toUnmodifiableSet());

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
      case RECOMMENDED ->
          RECOMMENDED_RULES.contains(issueCode) || !BUILT_IN_CODES.contains(issueCode);
      case MINIMAL -> SAFETY_CRITICAL.contains(issueCode);
    };
  }

  /**
   * Parses a configuration value. Case-insensitive; {@code null} and blank map to
   * {@link #RECOMMENDED}.
   *
   * @throws IllegalArgumentException on any other value, naming the accepted ones
   */
  public static RuleProfile parse(String value) {
    if (value == null || value.isBlank()) {
      return RECOMMENDED;
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
