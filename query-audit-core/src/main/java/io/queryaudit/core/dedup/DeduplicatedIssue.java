package io.queryaudit.core.dedup;

import io.queryaudit.core.model.Issue;
import io.queryaudit.core.model.Severity;
import java.util.List;

/**
 * Represents a unique issue that may appear across multiple test methods. Groups identical issues
 * by fingerprint and tracks how many tests are affected.
 *
 * @param issue representative issue (first occurrence)
 * @param fingerprint unique key: issueType|table|column|normalizedQuery
 * @param occurrenceCount total number of times this issue appeared across all tests
 * @param affectedTests test method names where this issue was found (max 10)
 * @param highestSeverity the most severe level seen for this fingerprint
 *
 * @author haroya
 * @since 0.2.0
 */
public record DeduplicatedIssue(
    Issue issue,
    String fingerprint,
    int occurrenceCount,
    List<String> affectedTests,
    Severity highestSeverity) {}
