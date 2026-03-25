package io.queryaudit.core.regression;

import io.queryaudit.core.model.QueryRecord;
import java.util.List;

/**
 * Immutable snapshot of query counts by type for a single test execution. All query types (SELECT,
 * INSERT, UPDATE, DELETE) are tracked.
 *
 * @author haroya
 * @since 0.2.0
 */
public record QueryCounts(
    int selectCount, int insertCount, int updateCount, int deleteCount, int totalCount) {

  /**
   * Builds a {@code QueryCounts} from a list of recorded queries. Classifies each query by its
   * leading SQL keyword.
   */
  public static QueryCounts from(List<QueryRecord> queries) {
    int select = 0, insert = 0, update = 0, delete = 0;
    for (QueryRecord q : queries) {
      String sql = q.sql();
      if (sql == null || sql.isBlank()) continue;
      String trimmed = sql.stripLeading().toLowerCase();
      if (trimmed.startsWith("select")) select++;
      else if (trimmed.startsWith("insert")) insert++;
      else if (trimmed.startsWith("update")) update++;
      else if (trimmed.startsWith("delete")) delete++;
    }
    return new QueryCounts(select, insert, update, delete, select + insert + update + delete);
  }
}
