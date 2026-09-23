package io.queryaudit.junit5;

import static org.assertj.core.api.Assertions.assertThat;

import io.queryaudit.core.regression.QueryCountBaseline;
import io.queryaudit.core.regression.QueryCounts;
import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class AuditPolicyFilesConcurrencyTest {
  @Test
  void concurrentClassCompletionPreservesBothClassesAndExistingEntries(@TempDir Path directory)
      throws Exception {
    Path path = directory.resolve("counts");
    var counts = new QueryCounts(1, 0, 0, 0, 1);
    QueryCountBaseline.save(path, Map.of(QueryCountBaseline.key("existing#test"), counts));
    var workers = Executors.newFixedThreadPool(2);
    var barrier = new CyclicBarrier(2);
    try {
      var first =
          workers.submit(
              () -> {
                for (int index = 0; index < 30; index++) {
                  barrier.await(10, TimeUnit.SECONDS);
                  AuditPolicyFiles.mergeAndSave(
                      path, Map.of(QueryCountBaseline.key("First#test" + index), counts), null);
                }
                return null;
              });
      var second =
          workers.submit(
              () -> {
                for (int index = 0; index < 30; index++) {
                  barrier.await(10, TimeUnit.SECONDS);
                  AuditPolicyFiles.mergeAndSave(
                      path,
                      Map.of(QueryCountBaseline.key("Second#test" + index), counts),
                      "QueryAudit Query Contracts");
                }
                return null;
              });
      first.get(20, TimeUnit.SECONDS);
      second.get(20, TimeUnit.SECONDS);
    } finally {
      workers.shutdownNow();
    }
    Map<String, QueryCounts> saved = QueryCountBaseline.load(path);
    assertThat(saved).hasSize(61).containsEntry(QueryCountBaseline.key("existing#test"), counts);
    for (int index = 0; index < 30; index++) {
      assertThat(saved)
          .containsEntry(QueryCountBaseline.key("First#test" + index), counts)
          .containsEntry(QueryCountBaseline.key("Second#test" + index), counts);
    }
  }
}
