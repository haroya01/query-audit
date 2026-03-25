package io.queryaudit.core.ranking;

import io.queryaudit.core.model.Issue;

/**
 * An issue enriched with its computed impact score, occurrence frequency, and rank position.
 *
 * @param issue the original issue
 * @param impactScore total impact score (higher = more impactful)
 * @param frequency how many times this issue pattern appears across tests
 * @param rank 1-based rank position (1 = highest impact)
 *
 * @author haroya
 * @since 0.2.0
 */
public record RankedIssue(Issue issue, int impactScore, int frequency, int rank) {}
