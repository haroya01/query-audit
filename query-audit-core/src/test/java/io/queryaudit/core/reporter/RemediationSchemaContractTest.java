package io.queryaudit.core.reporter;

import static org.assertj.core.api.Assertions.assertThat;

import io.queryaudit.core.model.Issue;
import io.queryaudit.core.model.IssueType;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;

class RemediationSchemaContractTest {

  private static final Path REPORT_SCHEMA =
      Path.of("..", "docs", "schema", "report-1.7.schema.json");

  @Test
  void schemaAcceptsEveryEmittedRemediationKind() throws IOException {
    Set<String> emittedKinds =
        Arrays.stream(IssueType.values())
            .map(RemediationSchemaContractTest::remediationFor)
            .filter(Objects::nonNull)
            .map(RemediationHints.Remediation::kind)
            .collect(Collectors.toSet());

    Map<?, ?> schema = (Map<?, ?>) MiniJsonParser.parse(Files.readString(REPORT_SCHEMA));
    Map<?, ?> definitions = (Map<?, ?>) schema.get("definitions");
    Map<?, ?> issue = (Map<?, ?>) definitions.get("issue");
    Map<?, ?> issueProperties = (Map<?, ?>) issue.get("properties");
    Map<?, ?> remediation = (Map<?, ?>) issueProperties.get("remediation");
    Map<?, ?> remediationProperties = (Map<?, ?>) remediation.get("properties");
    Map<?, ?> kind = (Map<?, ?>) remediationProperties.get("kind");
    Set<String> acceptedKinds =
        ((List<?>) kind.get("enum")).stream().map(String.class::cast).collect(Collectors.toSet());

    assertThat(acceptedKinds).containsExactlyInAnyOrderElementsOf(emittedKinds);
  }

  private static RemediationHints.Remediation remediationFor(IssueType type) {
    Issue issue =
        new Issue(
            type, type.getDefaultSeverity(), "SELECT 1", "orders", "id", "detail", "suggestion");
    return RemediationHints.forIssue(issue);
  }
}
