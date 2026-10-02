package io.queryaudit.junit5;

import static org.assertj.core.api.Assertions.assertThat;

import io.queryaudit.core.contract.QueryContractScope;
import io.queryaudit.core.interceptor.QueryInterceptor;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class ContractViolationsTest {

  @Test
  void findsAContractViolationThrownDirectlyWrappedOrSuppressed(@TempDir Path directory) {
    AssertionError violation = violation(directory);
    RuntimeException wrapped = new RuntimeException("request failed", violation);
    IllegalStateException primary = new IllegalStateException("body failed");
    primary.addSuppressed(violation(directory));

    assertThat(ContractViolations.causedBy(violation)).isTrue();
    assertThat(ContractViolations.causedBy(wrapped)).isTrue();
    assertThat(ContractViolations.causedBy(primary)).isTrue();
  }

  @Test
  void ignoresOtherAssertionsAndSurvivesCycles() {
    RuntimeException first = new RuntimeException("first");
    RuntimeException second = new RuntimeException("second", first);
    first.initCause(second);

    assertThat(ContractViolations.causedBy(new AssertionError("expected 2"))).isFalse();
    assertThat(ContractViolations.causedBy(first)).isFalse();
  }

  @Test
  void aPureContractViolationIsAPolicyFailureForCoverage(@TempDir Path directory) {
    AssertionError violation = violation(directory);
    IllegalStateException primary = new IllegalStateException("body failed");
    primary.addSuppressed(violation(directory));
    AssertionError withSuppressed = violation(directory);
    withSuppressed.addSuppressed(new IllegalStateException("cleanup failed"));

    assertThat(QueryAuditExtension.isAuditPolicyFailure(violation)).isTrue();
    assertThat(QueryAuditExtension.isAuditPolicyFailure(primary)).isFalse();
    assertThat(QueryAuditExtension.isAuditPolicyFailure(withSuppressed)).isFalse();
  }

  @Test
  void recordsEachFailedTestOnceInOrder() {
    ContractViolations violations = new ContractViolations();
    violations.record("[class:B]");
    violations.record("[class:A]");
    violations.record("[class:B]");

    assertThat(violations.tests()).containsExactly("[class:B]", "[class:A]");
  }

  private static AssertionError violation(Path directory) {
    try {
      Files.writeString(
          directory.resolve(".query-audit-contracts"), "@junit | read | 1 | 0 | 0 | 0 | 1\n");
      QueryContractScope.of(new QueryInterceptor(), directory).verify("read", () -> null);
    } catch (AssertionError expected) {
      return expected;
    } catch (Exception unexpected) {
      throw new IllegalStateException(unexpected);
    }
    throw new IllegalStateException("the scope did not reject the contract");
  }
}
