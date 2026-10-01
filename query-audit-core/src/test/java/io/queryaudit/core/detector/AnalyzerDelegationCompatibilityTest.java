package io.queryaudit.core.detector;

import static org.assertj.core.api.Assertions.assertThat;

import io.queryaudit.core.extension.FindingKindId;
import io.queryaudit.core.model.Finding;
import io.queryaudit.core.model.IndexMetadata;
import io.queryaudit.core.model.QueryAuditReport;
import io.queryaudit.core.model.QueryRecord;
import io.queryaudit.core.model.Severity;
import java.util.List;
import org.junit.jupiter.api.Test;

class AnalyzerDelegationCompatibilityTest {
  @Test
  void classAwareOverloadPreservesTheOriginalSubclassDispatchAndCustomFindings() {
    Finding finding =
        new Finding(
            FindingKindId.of("shop:override"),
            Severity.WARNING,
            null,
            null,
            null,
            "From an existing override",
            "Keep virtual dispatch");
    QueryAuditAnalyzer analyzer =
        new QueryAuditAnalyzer() {
          @Override
          public QueryAuditReport analyze(
              String name, List<QueryRecord> queries, IndexMetadata metadata) {
            return new QueryAuditReport("override-name", List.of(), List.of(), List.of(), 4, 5, 6)
                .withCustomFindings(List.of(finding), List.of(), List.of());
          }
        };

    QueryAuditReport report = analyzer.analyze("TestClass", "input-name", List.of(), null);
    assertThat(report.getTestClass()).isEqualTo("TestClass");
    assertThat(report.getTestName()).isEqualTo("override-name");
    assertThat(report.getFindings().confirmed()).containsExactly(finding);
    assertThat(report.getUniquePatternCount()).isEqualTo(4);
    assertThat(report.getTotalQueryCount()).isEqualTo(5);
    assertThat(report.getTotalExecutionTimeNanos()).isEqualTo(6);
  }
}
