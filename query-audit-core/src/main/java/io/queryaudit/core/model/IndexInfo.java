package io.queryaudit.core.model;

/**
 * Holds information about a single database index entry, including the table name, index name,
 * column name, position within the index, uniqueness, and cardinality. Used by index-aware
 * detectors to determine whether queries are properly indexed.
 *
 * @author haroya
 * @since 0.2.0
 */
public record IndexInfo(
    String tableName,
    String indexName,
    String columnName,
    int seqInIndex,
    boolean nonUnique,
    long cardinality) {}
