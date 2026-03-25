package io.queryaudit.core.analyzer;

import static org.assertj.core.api.Assertions.assertThat;

import io.queryaudit.core.model.IndexInfo;
import io.queryaudit.core.model.IndexMetadata;
import java.util.Collections;
import java.util.List;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

class JpaIndexScannerTest {

  // ── parseColumnList tests ────────────────────────────────────────────

  @Nested
  class ParseColumnListTest {

    @Test
    void singleColumn() {
      List<IndexInfo> result =
          JpaIndexScanner.parseColumnList("rooms", "idx_room_archived", "is_archived", true);

      assertThat(result).hasSize(1);
      IndexInfo info = result.get(0);
      assertThat(info.tableName()).isEqualTo("rooms");
      assertThat(info.indexName()).isEqualTo("idx_room_archived");
      assertThat(info.columnName()).isEqualTo("is_archived");
      assertThat(info.seqInIndex()).isEqualTo(1);
      assertThat(info.nonUnique()).isTrue();
      assertThat(info.cardinality()).isEqualTo(0);
    }

    @Test
    void multipleColumns() {
      List<IndexInfo> result =
          JpaIndexScanner.parseColumnList(
              "rooms", "idx_room_location_type", "location_id, type", true);

      assertThat(result).hasSize(2);

      assertThat(result.get(0).columnName()).isEqualTo("location_id");
      assertThat(result.get(0).seqInIndex()).isEqualTo(1);

      assertThat(result.get(1).columnName()).isEqualTo("type");
      assertThat(result.get(1).seqInIndex()).isEqualTo(2);
    }

    @Test
    void columnsWithSortDirection() {
      List<IndexInfo> result =
          JpaIndexScanner.parseColumnList(
              "orders", "idx_orders_date", "created_at DESC, status ASC", true);

      assertThat(result).hasSize(2);
      assertThat(result.get(0).columnName()).isEqualTo("created_at");
      assertThat(result.get(1).columnName()).isEqualTo("status");
    }

    @Test
    void emptyColumnList() {
      List<IndexInfo> result = JpaIndexScanner.parseColumnList("rooms", "idx_empty", "", true);
      assertThat(result).isEmpty();
    }

    @Test
    void nullColumnList() {
      List<IndexInfo> result = JpaIndexScanner.parseColumnList("rooms", "idx_null", null, true);
      assertThat(result).isEmpty();
    }

    @Test
    void uniqueIndex() {
      List<IndexInfo> result =
          JpaIndexScanner.parseColumnList("users", "idx_unique_email", "email", false);

      assertThat(result).hasSize(1);
      assertThat(result.get(0).nonUnique()).isFalse();
    }

    @Test
    void columnsWithExtraWhitespace() {
      List<IndexInfo> result =
          JpaIndexScanner.parseColumnList(
              "rooms", "idx_test", "  col_a  ,  col_b  ,  col_c  ", true);

      assertThat(result).hasSize(3);
      assertThat(result.get(0).columnName()).isEqualTo("col_a");
      assertThat(result.get(1).columnName()).isEqualTo("col_b");
      assertThat(result.get(2).columnName()).isEqualTo("col_c");
    }

    @Test
    void columnNamesAreLowercased() {
      List<IndexInfo> result =
          JpaIndexScanner.parseColumnList("rooms", "idx_test", "LocationId, TYPE", true);

      assertThat(result.get(0).columnName()).isEqualTo("locationid");
      assertThat(result.get(1).columnName()).isEqualTo("type");
    }
  }

  // ── camelToSnake tests ──────────────────────────────────────────────

  @Nested
  class CamelToSnakeTest {

    @Test
    void simpleClassName() {
      assertThat(JpaIndexScanner.camelToSnake("Room")).isEqualTo("room");
    }

    @Test
    void multiWordClassName() {
      assertThat(JpaIndexScanner.camelToSnake("RoomMember")).isEqualTo("room_member");
    }

    @Test
    void allLowercase() {
      assertThat(JpaIndexScanner.camelToSnake("room")).isEqualTo("room");
    }

    @Test
    void consecutiveUppercase() {
      assertThat(JpaIndexScanner.camelToSnake("HTMLParser")).isEqualTo("h_t_m_l_parser");
    }
  }

  // ── scan with empty collection ──────────────────────────────────────

  @Nested
  class ScanTest {

    @Test
    void emptyClassList() {
      JpaIndexScanner scanner = new JpaIndexScanner();
      IndexMetadata metadata = scanner.scan(Collections.emptyList());
      assertThat(metadata.isEmpty()).isTrue();
    }

    @Test
    void nonEntityClass() {
      JpaIndexScanner scanner = new JpaIndexScanner();
      IndexMetadata metadata = scanner.scan(List.of(String.class, Integer.class));
      assertThat(metadata.isEmpty()).isTrue();
    }
  }
}
