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
    String roomId =
        "[engine:junit-jupiter]/[class:com.example.RoomApiTest]/[method:testCreateRoom()]";
    String messageId =
        "[engine:junit-jupiter]/[class:com.example.MessageApiTest]/[method:testSendMessage()]";
    counts.put(QueryCountBaseline.key(roomId), new QueryCounts(12, 3, 0, 0, 15));
    counts.put(QueryCountBaseline.key(messageId), new QueryCounts(15, 2, 1, 0, 18));

    QueryCountBaseline.save(file, counts);

    Map<String, QueryCounts> loaded = QueryCountBaseline.load(file);
    String saved = Files.readString(file);

    assertThat(loaded).hasSize(2);
    assertThat(saved).contains("# Format: identityType | identityValue");
    assertThat(saved).contains("@junit | " + roomId + " | 12 | 3 | 0 | 0 | 15");

    QueryCounts room = loaded.get(QueryCountBaseline.key(roomId));
    assertThat(room).isNotNull();
    assertThat(room.selectCount()).isEqualTo(12);
    assertThat(room.insertCount()).isEqualTo(3);
    assertThat(room.updateCount()).isEqualTo(0);
    assertThat(room.deleteCount()).isEqualTo(0);
    assertThat(room.totalCount()).isEqualTo(15);

    QueryCounts message = loaded.get(QueryCountBaseline.key(messageId));
    assertThat(message).isNotNull();
    assertThat(message.selectCount()).isEqualTo(15);
    assertThat(message.totalCount()).isEqualTo(18);
  }

  @Test
  void stableIdentitySpecialCharactersRoundTripThroughReadableEscapes() throws IOException {
    Path file = tempDir.resolve(".query-audit-counts");
    String testId = "[engine:junit-jupiter]/[method:case|path\\segment\rfirst line\nsecond line()]";
    QueryCounts expectedCounts = new QueryCounts(2, 1, 0, 0, 3);

    QueryCountBaseline.save(file, Map.of(QueryCountBaseline.key(testId), expectedCounts));

    String saved = Files.readString(file);
    assertThat(saved)
        .contains(
            "@junit | [engine:junit-jupiter]/[method:case\\|path\\\\segment\\rfirst line\\nsecond line()] | 2 | 1 | 0 | 0 | 3")
        .contains(
            "# @junit identityValue escapes: \\| (pipe), \\\\ (backslash), \\r (CR), \\n (LF)");
    assertThat(Files.readAllLines(file)).hasSize(4);
    assertThat(QueryCountBaseline.load(file))
        .containsEntry(QueryCountBaseline.key(testId), expectedCounts);
  }

  @Test
  void loadDecodesEscapedStableIdentityFields() throws IOException {
    Path file = tempDir.resolve(".query-audit-counts");
    Files.writeString(
        file,
        "@junit | [engine:junit-jupiter]/[method:pipe\\|slash\\\\cr\\rline\\n()]"
            + " | 1 | 0 | 0 | 0 | 1\n");
    String testId = "[engine:junit-jupiter]/[method:pipe|slash\\cr\rline\n()]";

    assertThat(QueryCountBaseline.load(file))
        .containsEntry(QueryCountBaseline.key(testId), new QueryCounts(1, 0, 0, 0, 1));
  }

  @Test
  void legacyRowsKeepRawBackslashSequences() throws IOException {
    Path file = tempDir.resolve(".query-audit-counts");
    String legacyDisplayName = "windows\\path\\nremains-raw";
    Files.writeString(file, "LegacyTest | " + legacyDisplayName + " | 1 | 0 | 0 | 0 | 1\n");

    assertThat(QueryCountBaseline.load(file))
        .containsEntry(
            QueryCountBaseline.key("LegacyTest", legacyDisplayName),
            new QueryCounts(1, 0, 0, 0, 1));
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
  void loadRejectsUnknownStableIdentityEscapesWithLocation() throws IOException {
    Path file = tempDir.resolve(".query-audit-counts");
    Files.writeString(
        file,
        "@junit | [engine:junit-jupiter]/[method:unsupported\\q()]" + " | 1 | 0 | 0 | 0 | 1\n");

    assertThatThrownBy(() -> QueryCountBaseline.load(file))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining(file.toAbsolutePath().toString())
        .hasMessageContaining("line 1")
        .hasMessageContaining("unsupported @junit identityValue escape \\q")
        .hasMessageContaining("expected one of \\|, \\\\, \\r, \\n");
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
  void stableEntriesDistinguishTestsWithTheSameLegacyName() throws IOException {
    Path file = tempDir.resolve(".query-audit-counts");
    String firstId = "[engine:junit-jupiter]/[class:com.alpha.OrderTest]/[method:loadsOrders()]";
    String secondId = "[engine:junit-jupiter]/[class:com.beta.OrderTest]/[method:loadsOrders()]";

    QueryCountBaseline.save(
        file,
        Map.of(
            QueryCountBaseline.key(firstId), new QueryCounts(1, 0, 0, 0, 1),
            QueryCountBaseline.key(secondId), new QueryCounts(2, 0, 0, 0, 2)));

    Map<String, QueryCounts> loaded = QueryCountBaseline.load(file);
    assertThat(QueryCountBaseline.find(loaded, firstId, "OrderTest", "loadsOrders"))
        .isEqualTo(new QueryCounts(1, 0, 0, 0, 1));
    assertThat(QueryCountBaseline.find(loaded, secondId, "OrderTest", "loadsOrders"))
        .isEqualTo(new QueryCounts(2, 0, 0, 0, 2));
  }

  @Test
  void stableIdentityWinsOverALegacyDisplayNameEntry() {
    String testId = "[engine:junit-jupiter]/[class:example.OrderTest]/[method:loadsOrders()]";
    Map<String, QueryCounts> counts =
        Map.of(
            QueryCountBaseline.key(testId), new QueryCounts(1, 0, 0, 0, 1),
            QueryCountBaseline.key("OrderTest", "loads orders"), new QueryCounts(9, 0, 0, 0, 9));

    assertThat(QueryCountBaseline.find(counts, testId, "OrderTest", "loads orders"))
        .isEqualTo(new QueryCounts(1, 0, 0, 0, 1));
    assertThat(QueryCountBaseline.hasLegacyIdentity(counts, "OrderTest", "loads orders")).isTrue();
    assertThat(QueryCountBaseline.usesLegacyIdentity(counts, testId, "OrderTest", "loads orders"))
        .isFalse();
  }

  @Test
  void legacyEntryRemainsAnExplicitFallback() {
    String testId = "[engine:junit-jupiter]/[class:example.OrderTest]/[method:loadsOrders()]";
    Map<String, QueryCounts> counts =
        Map.of(QueryCountBaseline.key("OrderTest", "loads orders"), new QueryCounts(4, 0, 0, 0, 4));

    assertThat(QueryCountBaseline.find(counts, testId, "OrderTest", "loads orders"))
        .isEqualTo(new QueryCounts(4, 0, 0, 0, 4));
    assertThat(QueryCountBaseline.usesLegacyIdentity(counts, testId, "OrderTest", "loads orders"))
        .isTrue();
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

  @Test
  void stableKeyUsesTheBackwardCompatibleIdentityPair() {
    String testId = "[engine:junit-jupiter]/[class:example.RoomApiTest]/[method:create()]";

    assertThat(QueryCountBaseline.key(testId)).isEqualTo("@junit|" + testId);
  }
}
