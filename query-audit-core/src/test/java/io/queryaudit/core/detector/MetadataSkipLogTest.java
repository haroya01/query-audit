package io.queryaudit.core.detector;

import static org.assertj.core.api.Assertions.assertThat;

import io.queryaudit.core.model.IndexMetadata;
import io.queryaudit.core.model.QueryRecord;
import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Regression for issue #96 — metadata-dependent detectors must log a one-shot warning when they
 * disable themselves due to empty/null {@link IndexMetadata}, so users have a trail to follow
 * when a rule mysteriously stops firing.
 */
@DisplayName("MetadataSkipLog — warn-once contract (issue #96)")
class MetadataSkipLogTest {

  private static final IndexMetadata EMPTY = new IndexMetadata(Map.of());

  private PrintStream originalErr;
  private ByteArrayOutputStream capturedErr;

  @BeforeEach
  void setUp() {
    originalErr = System.err;
    capturedErr = new ByteArrayOutputStream();
    System.setErr(new PrintStream(capturedErr));
    MetadataSkipLog.resetForTesting();
  }

  @AfterEach
  void tearDown() {
    System.setErr(originalErr);
    MetadataSkipLog.resetForTesting();
  }

  private static QueryRecord record(String sql) {
    return new QueryRecord(sql, 0L, System.currentTimeMillis(), "");
  }

  @Test
  @DisplayName("first empty-metadata invocation emits a warning; subsequent calls stay silent")
  void warnsOncePerDetector() {
    OrderByLimitWithoutIndexDetector detector = new OrderByLimitWithoutIndexDetector();
    QueryRecord q = record("SELECT * FROM t ORDER BY x LIMIT 10");

    detector.evaluate(List.of(q), EMPTY);
    detector.evaluate(List.of(q), EMPTY);
    detector.evaluate(List.of(q), null);

    String err = capturedErr.toString();
    assertThat(err).contains("IndexMetadata is empty");
    assertThat(err).contains("OrderByLimitWithoutIndexDetector");
    // Only one occurrence of the warning despite three empty-metadata calls.
    int occurrences = err.split("OrderByLimitWithoutIndexDetector", -1).length - 1;
    assertThat(occurrences).isEqualTo(1);
  }

  @Test
  @DisplayName("each distinct detector gets its own warning the first time it skips")
  void warnsSeparatelyPerDetector() {
    OrderByLimitWithoutIndexDetector d1 = new OrderByLimitWithoutIndexDetector();
    CompositeIndexDetector d2 = new CompositeIndexDetector();
    MissingIndexDetector d3 = new MissingIndexDetector();
    NonDeterministicPaginationDetector d4 = new NonDeterministicPaginationDetector();
    QueryRecord q = record("SELECT * FROM t WHERE x = 1 ORDER BY id LIMIT 10");

    d1.evaluate(List.of(q), EMPTY);
    d2.evaluate(List.of(q), EMPTY);
    d3.evaluate(List.of(q), EMPTY);
    d4.evaluate(List.of(q), EMPTY);

    String err = capturedErr.toString();
    assertThat(err).contains("OrderByLimitWithoutIndexDetector");
    assertThat(err).contains("CompositeIndexDetector");
    assertThat(err).contains("MissingIndexDetector");
    assertThat(err).contains("NonDeterministicPaginationDetector");
  }
}
