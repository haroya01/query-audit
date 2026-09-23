package io.queryaudit.core.contract;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.queryaudit.core.interceptor.QueryInterceptor;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import net.ttddyy.dsproxy.ExecutionInfo;
import net.ttddyy.dsproxy.QueryInfo;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.parallel.ResourceLock;
import org.junit.jupiter.api.parallel.Resources;

@ResourceLock(Resources.SYSTEM_PROPERTIES)
class QueryContractScopeTest {
  @TempDir Path directory;
  private final QueryInterceptor interceptor = new QueryInterceptor();

  @AfterEach
  void clearRecordMode() {
    System.clearProperty(QueryContractScope.RECORD_PROPERTY);
  }

  @Test
  void returnsTheWorkResultWhenTheReviewedContractMatches() throws Exception {
    write("api.contracts", "# Reviewed against MySQL", "@junit | link-list | 2 | 0 | 0 | 0 | 2");

    String body =
        scope()
            .verify(
                "link-list",
                () -> {
                  fire("SELECT * FROM links WHERE owner_id = ?");
                  fire("SELECT count(*) FROM links WHERE owner_id = ?");
                  return "ok";
                });

    assertThat(body).isEqualTo("ok");
  }

  @Test
  void failsWithTheDeltaAndTheOffendingSqlWhenCountsChange() {
    write("api.contracts", "@junit | link-list | 1 | 0 | 0 | 0 | 1");

    assertThatThrownBy(
            () ->
                scope()
                    .verify(
                        "link-list",
                        () -> {
                          fire("SELECT * FROM links WHERE owner_id = ?");
                          fire("UPDATE links SET viewed_at = ? WHERE id = ?");
                          return null;
                        }))
        .isInstanceOf(AssertionError.class)
        .hasMessageContaining("link-list deviates from its recorded query contract")
        .hasMessageContaining("UPDATE: contract 0, executed 1 (+1)")
        .hasMessageContaining("UPDATE links SET viewed_at");
  }

  @Test
  void failsWhenNoContractWasReviewedForTheScope() {
    write("api.contracts", "@junit | link-list | 1 | 0 | 0 | 0 | 1");

    assertThatThrownBy(() -> scope().verify("link-create", () -> null))
        .isInstanceOf(AssertionError.class)
        .hasMessageContaining("Missing query contract: link-create")
        .hasMessageContaining(QueryContractScope.RECORD_PROPERTY);
  }

  @Test
  void refusesToVerifyATruncatedCapture() {
    write("api.contracts", "@junit | link-list | 1 | 0 | 0 | 0 | 1");
    interceptor.setMaxQueries(1);

    assertThatThrownBy(
            () ->
                scope()
                    .verify(
                        "link-list",
                        () -> {
                          fire("SELECT 1");
                          fire("SELECT 2");
                          return null;
                        }))
        .isInstanceOf(AssertionError.class)
        .hasMessageContaining("discarded 1 queries");
  }

  @Test
  void recordModeUpdatesTheOwningFileAndAddsNewScopesToTheDefaultFile() throws Exception {
    write("api.contracts", "@junit | link-list | 1 | 0 | 0 | 0 | 1");
    write("jobs.contracts", "@junit | click-flush | 0 | 1 | 0 | 0 | 1");
    System.setProperty(QueryContractScope.RECORD_PROPERTY, "true");

    scope()
        .verify(
            "link-list",
            () -> {
              fire("SELECT 1");
              fire("SELECT 2");
              return null;
            });
    scope().verify("link-create", () -> fire("INSERT INTO links (code) VALUES (?)"));

    assertThat(Files.readString(directory.resolve("api.contracts")))
        .contains("@junit | link-list | 2 | 0 | 0 | 0 | 2");
    assertThat(Files.readString(directory.resolve("jobs.contracts")))
        .contains("@junit | click-flush | 0 | 1 | 0 | 0 | 1");
    assertThat(Files.readString(directory.resolve(".query-audit-contracts")))
        .contains("@junit | link-create | 0 | 1 | 0 | 0 | 1");

    System.clearProperty(QueryContractScope.RECORD_PROPERTY);
    scope().verify("link-create", () -> fire("INSERT INTO links (code) VALUES (?)"));
  }

  @Test
  void rejectsAScopeIdReviewedInTwoFiles() {
    write("a.contracts", "@junit | link-list | 1 | 0 | 0 | 0 | 1");
    write("b.contracts", "@junit | link-list | 1 | 0 | 0 | 0 | 1");

    assertThatThrownBy(() -> scope().verify("link-list", () -> fire("SELECT 1")))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("Duplicate query contract");
  }

  @Test
  void refusesToNestInsideAnotherRunningCapture() {
    interceptor.start();
    try {
      assertThatThrownBy(() -> scope().capture("link-list", () -> null))
          .isInstanceOf(IllegalStateException.class)
          .hasMessageContaining("already running");
    } finally {
      interceptor.stop();
    }
  }

  @Test
  void countsBackgroundWorkThatFinishesBeforeTheScopeCloses() throws Exception {
    write("jobs.contracts", "@junit | link-create | 1 | 1 | 0 | 0 | 2");
    ExecutorService executor = Executors.newSingleThreadExecutor();
    CountDownLatch done = new CountDownLatch(1);
    try {
      scope()
          .awaitingCompletion(() -> await(done))
          .verify(
              "link-create",
              () -> {
                fire("INSERT INTO links (code) VALUES (?)");
                executor.submit(
                    () -> {
                      fire("SELECT * FROM link_webhooks WHERE link_id = ?");
                      done.countDown();
                    });
                return null;
              });
    } finally {
      executor.shutdownNow();
    }
  }

  @Test
  void exposesTheCapturedQueriesAndCounts() throws Exception {
    CapturedQueries<Integer> captured =
        scope()
            .capture(
                "link-list",
                () -> {
                  fire("SELECT 1");
                  return 7;
                });

    assertThat(captured.value()).isEqualTo(7);
    assertThat(captured.queries()).hasSize(1);
    assertThat(captured.counts().selectCount()).isEqualTo(1);
  }

  private QueryContractScope scope() {
    return QueryContractScope.forDirectory(interceptor, directory);
  }

  private Object fire(String sql) {
    ExecutionInfo execution = new ExecutionInfo();
    execution.setElapsedTime(1L);
    interceptor.afterQuery(execution, List.of(new QueryInfo(sql)));
    return null;
  }

  private void write(String file, String... lines) {
    try {
      Files.write(directory.resolve(file), List.of(lines));
    } catch (Exception failure) {
      throw new IllegalStateException(failure);
    }
  }

  private static void await(CountDownLatch latch) {
    try {
      assertThat(latch.await(5, TimeUnit.SECONDS)).isTrue();
    } catch (InterruptedException interrupted) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException(interrupted);
    }
  }
}
