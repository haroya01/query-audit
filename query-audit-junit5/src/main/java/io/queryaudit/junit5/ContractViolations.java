package io.queryaudit.junit5;

import io.queryaudit.core.contract.QueryContractViolation;
import java.util.ArrayDeque;
import java.util.Collections;
import java.util.Deque;
import java.util.IdentityHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/** Tests of one platform execution that ended with a scoped query contract failure. */
final class ContractViolations {
  private final Set<String> tests = new LinkedHashSet<>();

  synchronized void record(String testId) {
    tests.add(testId);
  }

  synchronized List<String> tests() {
    return List.copyOf(tests);
  }

  static boolean causedBy(Throwable failure) {
    Set<Throwable> seen = Collections.newSetFromMap(new IdentityHashMap<>());
    Deque<Throwable> pending = new ArrayDeque<>();
    pending.push(failure);
    while (!pending.isEmpty()) {
      Throwable current = pending.pop();
      if (!seen.add(current)) continue;
      if (current instanceof QueryContractViolation) return true;
      if (current.getCause() != null) pending.push(current.getCause());
      for (Throwable suppressed : current.getSuppressed()) pending.push(suppressed);
    }
    return false;
  }
}
