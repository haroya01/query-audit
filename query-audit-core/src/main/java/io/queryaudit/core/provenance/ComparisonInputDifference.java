package io.queryaudit.core.provenance;

/**
 * One incompatible or unavailable input, without exposing underlying policy contents.
 *
 * @since 0.6.0
 */
public record ComparisonInputDifference(
    String testId, String field, String baseline, String candidate) {}
