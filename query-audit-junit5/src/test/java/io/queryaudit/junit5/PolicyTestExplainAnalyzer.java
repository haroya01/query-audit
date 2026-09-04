package io.queryaudit.junit5;

import io.queryaudit.core.analyzer.ExplainAnalysisException;
import io.queryaudit.core.analyzer.ExplainAnalyzer;
import io.queryaudit.core.model.Issue;
import io.queryaudit.core.model.IssueType;
import io.queryaudit.core.model.QueryRecord;
import io.queryaudit.core.model.Severity;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;

public final class PolicyTestExplainAnalyzer implements ExplainAnalyzer {

  @Override
  public String supportedDatabase() {
    return "policy test db";
  }

  @Override
  public List<Issue> analyze(Connection connection, List<QueryRecord> queries) {
    if (queries.get(0).sql().contains("private_marker")) {
      throw new ExplainAnalysisException(List.of(), new SQLException("private_marker"));
    }
    return List.of(
        new Issue(
            IssueType.FILESORT,
            Severity.INFO,
            queries.get(0).sql(),
            "orders",
            null,
            "EXPLAIN reports a filesort",
            "Add an index that supports the ordering"));
  }
}
