package io.queryaudit.core.extension;

import io.queryaudit.core.model.IndexMetadata;
import io.queryaudit.core.model.QueryRecord;
import java.util.List;
import java.util.Map;

/**
 * Host-scoped evidence for one rule evaluation. Queries have already passed query suppression and
 * lifecycle selection. Collections and index metadata are snapshots; absent metadata is empty. The
 * host, not a rule, owns severity, suppression, baseline, privacy, and completeness decisions.
 */
public record RuleContext(List<QueryRecord> queries, IndexMetadata indexMetadata) {
  public RuleContext {
    queries = List.copyOf(queries);
    indexMetadata = indexMetadata == null ? new IndexMetadata(Map.of()) : indexMetadata.snapshot();
  }
}
