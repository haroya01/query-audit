package io.queryaudit.core.interceptor;

import io.queryaudit.core.model.QueryRecord;
import java.util.List;
import java.util.Objects;

/**
 * Immutable snapshot of the queries retained during one capture window and the number discarded
 * after the configured capacity was exhausted.
 *
 * @param queries retained queries
 * @param droppedCount number of queries discarded because the capture limit was exceeded
 * @since 0.6.0
 */
public record QueryCaptureSnapshot(List<QueryRecord> queries, long droppedCount) {

  public QueryCaptureSnapshot {
    Objects.requireNonNull(queries, "queries");
    if (droppedCount < 0) {
      throw new IllegalArgumentException("droppedCount must not be negative: " + droppedCount);
    }
    queries = List.copyOf(queries);
  }

  /** Returns whether at least one query was discarded from this capture window. */
  public boolean truncated() {
    return droppedCount > 0;
  }
}
