package io.queryaudit.core.regression;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.Map;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class QueryCountBaselineTest {

  @TempDir Path tempDir;

  @Test
  void saveAndLoadRoundTrip() throws IOException {
    Path file = tempDir.resolve(".query-audit-counts");

    Map<String, QueryCounts> counts = new LinkedHashMap<>();
    counts.put(
        QueryCountBaseline.key("RoomApiTest", "testCreateRoom"), new QueryCounts(12, 3, 0, 0, 15));
    counts.put(
        QueryCountBaseline.key("MessageApiTest", "testSendMessage"),
        new QueryCounts(15, 2, 1, 0, 18));

    QueryCountBaseline.save(file, counts);

    Map<String, QueryCounts> loaded = QueryCountBaseline.load(file);

    assertThat(loaded).hasSize(2);

    QueryCounts room = loaded.get(QueryCountBaseline.key("RoomApiTest", "testCreateRoom"));
    assertThat(room).isNotNull();
    assertThat(room.selectCount()).isEqualTo(12);
    assertThat(room.insertCount()).isEqualTo(3);
    assertThat(room.updateCount()).isEqualTo(0);
    assertThat(room.deleteCount()).isEqualTo(0);
    assertThat(room.totalCount()).isEqualTo(15);

    QueryCounts message = loaded.get(QueryCountBaseline.key("MessageApiTest", "testSendMessage"));
    assertThat(message).isNotNull();
    assertThat(message.selectCount()).isEqualTo(15);
    assertThat(message.totalCount()).isEqualTo(18);
  }

  @Test
  void loadReturnsEmptyMapWhenFileDoesNotExist() {
    Map<String, QueryCounts> loaded = QueryCountBaseline.load(tempDir.resolve("nonexistent"));
    assertThat(loaded).isEmpty();
  }

  @Test
  void loadReturnsEmptyMapWhenPathIsNull() {
    Map<String, QueryCounts> loaded = QueryCountBaseline.load(null);
    assertThat(loaded).isEmpty();
  }

  @Test
  void loadRejectsLinesWithTheWrongNumberOfFields() throws IOException {
    Path file = tempDir.resolve(".query-audit-counts");
    Files.writeString(
        file,
        """
                # Header comment
                RoomApiTest | testCreateRoom | 12 | 3 | 0 | 0 | 15
                bad line without enough pipes
                """);

    assertThatThrownBy(() -> QueryCountBaseline.load(file))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining(file.toAbsolutePath().toString())
        .hasMessageContaining("line 3")
        .hasMessageContaining("expected 7 pipe-separated fields, found 1");
  }

  @Test
  void loadRejectsInvalidCounts() throws IOException {
    Path file = tempDir.resolve(".query-audit-counts");
    Files.writeString(file, "MessageApiTest | testSendMessage | not-a-number | 2 | 1 | 0 | 18\n");

    assertThatThrownBy(() -> QueryCountBaseline.load(file))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining(file.toAbsolutePath().toString())
        .hasMessageContaining("line 1")
        .hasMessageContaining("selectCount must be an integer: not-a-number");
  }

  @Test
  void loadRejectsNegativeCounts() throws IOException {
    Path file = tempDir.resolve(".query-audit-counts");
    Files.writeString(file, "MessageApiTest | testSendMessage | 1 | -1 | 0 | 0 | 0\n");

    assertThatThrownBy(() -> QueryCountBaseline.load(file))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("line 1")
        .hasMessageContaining("insertCount must not be negative: -1");
  }

  @Test
  void loadRejectsAnInconsistentTotal() throws IOException {
    Path file = tempDir.resolve(".query-audit-counts");
    Files.writeString(file, "MessageApiTest | testSendMessage | 1 | 2 | 0 | 0 | 4\n");

    assertThatThrownBy(() -> QueryCountBaseline.load(file))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("line 1")
        .hasMessageContaining("expected 3, found 4");
  }

  @Test
  void loadRejectsAnExistingPathThatIsNotAFile() {
    assertThatThrownBy(() -> QueryCountBaseline.load(tempDir))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining(tempDir.toAbsolutePath().toString())
        .hasMessageContaining("Cannot read QueryAudit policy file");
  }

  @Test
  void loadRejectsADanglingSymbolicLink() {
    Path file = tempDir.resolve(".query-audit-counts");
    try {
      Files.createSymbolicLink(file, tempDir.resolve("missing-counts"));
    } catch (IOException | UnsupportedOperationException e) {
      Assumptions.abort("Symbolic links are unavailable: " + e.getMessage());
    }

    assertThatThrownBy(() -> QueryCountBaseline.load(file))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining(file.toAbsolutePath().toString())
        .hasMessageContaining("Cannot read QueryAudit policy file");
  }

  @Test
  void loadRejectsDuplicateTestEntries() throws IOException {
    Path file = tempDir.resolve(".query-audit-counts");
    Files.writeString(
        file,
        """
                OrderServiceTest | loadsOrders | 1 | 0 | 0 | 0 | 1
                OrderServiceTest | loadsOrders | 2 | 0 | 0 | 0 | 2
                """);

    assertThatThrownBy(() -> QueryCountBaseline.load(file))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("line 2")
        .hasMessageContaining("duplicate entry for OrderServiceTest|loadsOrders");
  }

  @Test
  void loadSkipsBlankLinesAndComments() throws IOException {
    Path file = tempDir.resolve(".query-audit-counts");
    Files.writeString(
        file,
        """
                # Query Guard Count Baseline
                # Format: testClass | testMethod | select | insert | update | delete | total

                RoomApiTest | testCreateRoom | 10 | 0 | 0 | 0 | 10

                # Another comment
                UserApiTest | testGetUser | 5 | 0 | 0 | 0 | 5
                """);

    Map<String, QueryCounts> loaded = QueryCountBaseline.load(file);
    assertThat(loaded).hasSize(2);
  }

  @Test
  void savedFileIsHumanReadable() throws IOException {
    Path file = tempDir.resolve(".query-audit-counts");

    Map<String, QueryCounts> counts = new LinkedHashMap<>();
    counts.put(
        QueryCountBaseline.key("RoomApiTest", "testCreateRoom"), new QueryCounts(12, 3, 0, 0, 15));

    QueryCountBaseline.save(file, counts);

    String content = Files.readString(file);
    assertThat(content).contains("# Query Guard Count Baseline");
    assertThat(content).contains("RoomApiTest | testCreateRoom | 12 | 3 | 0 | 0 | 15");
  }

  @Test
  void savedFileIsSortedByKey() throws IOException {
    Path file = tempDir.resolve(".query-audit-counts");

    Map<String, QueryCounts> counts = new LinkedHashMap<>();
    counts.put(QueryCountBaseline.key("ZTest", "methodB"), new QueryCounts(5, 0, 0, 0, 5));
    counts.put(QueryCountBaseline.key("ATest", "methodA"), new QueryCounts(3, 0, 0, 0, 3));

    QueryCountBaseline.save(file, counts);

    String content = Files.readString(file);
    int posA = content.indexOf("ATest");
    int posZ = content.indexOf("ZTest");
    assertThat(posA).isLessThan(posZ);
  }

  @Test
  void keyFormatIsCorrect() {
    String key = QueryCountBaseline.key("RoomApiTest", "testCreateRoom");
    assertThat(key).isEqualTo("RoomApiTest|testCreateRoom");
  }
}
