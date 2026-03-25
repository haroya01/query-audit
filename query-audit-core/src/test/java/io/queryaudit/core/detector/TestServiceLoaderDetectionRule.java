package io.queryaudit.core.detector;

import io.queryaudit.core.model.IndexMetadata;
import io.queryaudit.core.model.Issue;
import io.queryaudit.core.model.IssueType;
import io.queryaudit.core.model.QueryRecord;
import io.queryaudit.core.model.Severity;
import java.util.ArrayList;
import java.util.List;

/**
 * A test-only detection rule discovered via ServiceLoader. Registered in
 * {@code META-INF/services/io.queryaudit.core.detector.DetectionRule} under test resources.
 *
 * <p>Flags any query containing the keyword "SERVICELOADER_TEST".
 */
public class TestServiceLoaderDetectionRule implements DetectionRule {

  @Override
  public List<Issue> evaluate(List<QueryRecord> queries, IndexMetadata indexMetadata) {
    List<Issue> issues = new ArrayList<>();
    for (QueryRecord query : queries) {
      if (query.sql() != null && query.sql().toUpperCase().contains("SERVICELOADER_TEST")) {
        issues.add(
            new Issue(
                IssueType.SELECT_ALL, // reuse existing type for test simplicity
                Severity.WARNING,
                query.sql(),
                null,
                null,
                "ServiceLoader-discovered rule triggered",
                "This is a test rule discovered via ServiceLoader"));
      }
    }
    return issues;
  }
}
