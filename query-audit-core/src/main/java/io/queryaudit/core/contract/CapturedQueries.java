package io.queryaudit.core.contract;

import io.queryaudit.core.interceptor.QueryCaptureSnapshot;
import io.queryaudit.core.model.QueryRecord;
import io.queryaudit.core.regression.QueryCounts;
import java.util.List;
import java.util.Objects;

public record CapturedQueries<T>(String contractId, T value, QueryCaptureSnapshot snapshot) {

  public CapturedQueries {
    Objects.requireNonNull(contractId, "contractId");
    Objects.requireNonNull(snapshot, "snapshot");
  }

  public List<QueryRecord> queries() {
    return snapshot.queries();
  }

  public QueryCounts counts() {
    return QueryCounts.from(snapshot.queries());
  }
}
