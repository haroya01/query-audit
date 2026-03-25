package io.queryaudit.core.parser;

/**
 * Represents a pair of column references from a JOIN ON condition (e.g., {@code a.id = b.a_id}).
 * Used by index detectors to verify that both sides of a join are properly indexed.
 *
 * @author haroya
 * @since 0.2.0
 */
public record JoinColumnPair(ColumnReference left, ColumnReference right) {}
