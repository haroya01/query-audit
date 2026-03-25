package io.queryaudit.core.model;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.*;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

class IndexMetadataMergeTest {

  private static IndexInfo idx(
      String table, String name, String column, int seq, boolean nonUnique) {
    return new IndexInfo(table, name, column, seq, nonUnique, 100);
  }

  @Nested
  class MergeBasicTest {

    @Test
    void mergeNullReturnsThis() {
      IndexMetadata primary =
          new IndexMetadata(Map.of("rooms", List.of(idx("rooms", "PRIMARY", "id", 1, false))));
      IndexMetadata result = primary.merge(null);
      assertThat(result).isSameAs(primary);
    }

    @Test
    void mergeEmptyReturnsThis() {
      IndexMetadata primary =
          new IndexMetadata(Map.of("rooms", List.of(idx("rooms", "PRIMARY", "id", 1, false))));
      IndexMetadata result = primary.merge(new IndexMetadata(Map.of()));
      assertThat(result).isSameAs(primary);
    }

    @Test
    void mergeIntoEmptyReturnsOther() {
      IndexMetadata empty = new IndexMetadata(Map.of());
      IndexMetadata other =
          new IndexMetadata(
              Map.of("rooms", List.of(idx("rooms", "idx_room_archived", "is_archived", 1, true))));
      IndexMetadata result = empty.merge(other);
      assertThat(result).isSameAs(other);
    }
  }

  @Nested
  class MergeConflictTest {

    @Test
    void sameIndexNamePrefersThisPrimary() {
      IndexMetadata primary =
          new IndexMetadata(
              Map.of("rooms", List.of(idx("rooms", "idx_room_archived", "is_archived", 1, true))));
      IndexMetadata jpa =
          new IndexMetadata(
              Map.of(
                  "rooms",
                  List.of(
                      // Same index name but different cardinality (JPA defaults to 0)
                      new IndexInfo("rooms", "idx_room_archived", "is_archived", 1, true, 0))));

      IndexMetadata result = primary.merge(jpa);
      List<IndexInfo> roomIndexes = result.getIndexesForTable("rooms");

      // Should only have one entry (from primary), not duplicated
      long count =
          roomIndexes.stream().filter(i -> "idx_room_archived".equals(i.indexName())).count();
      assertThat(count).isEqualTo(1);

      // Should keep the primary version (cardinality 100, not 0)
      IndexInfo kept =
          roomIndexes.stream()
              .filter(i -> "idx_room_archived".equals(i.indexName()))
              .findFirst()
              .orElseThrow();
      assertThat(kept.cardinality()).isEqualTo(100);
    }

    @Test
    void differentIndexNamesAreMerged() {
      IndexMetadata primary =
          new IndexMetadata(
              Map.of("rooms", new ArrayList<>(List.of(idx("rooms", "PRIMARY", "id", 1, false)))));
      IndexMetadata jpa =
          new IndexMetadata(
              Map.of(
                  "rooms",
                  List.of(
                      idx("rooms", "idx_room_archived", "is_archived", 1, true),
                      idx("rooms", "idx_room_expires_at", "expires_at", 1, true))));

      IndexMetadata result = primary.merge(jpa);
      List<IndexInfo> roomIndexes = result.getIndexesForTable("rooms");

      assertThat(roomIndexes).hasSize(3);
      assertThat(roomIndexes.stream().map(IndexInfo::indexName))
          .containsExactlyInAnyOrder("PRIMARY", "idx_room_archived", "idx_room_expires_at");
    }
  }

  @Nested
  class MergeDifferentTablesTest {

    @Test
    void tablesFromBothSourcesAreIncluded() {
      IndexMetadata primary =
          new IndexMetadata(Map.of("rooms", List.of(idx("rooms", "PRIMARY", "id", 1, false))));
      IndexMetadata jpa =
          new IndexMetadata(
              Map.of("users", List.of(idx("users", "idx_user_email", "email", 1, false))));

      IndexMetadata result = primary.merge(jpa);

      assertThat(result.hasTable("rooms")).isTrue();
      assertThat(result.hasTable("users")).isTrue();
      assertThat(result.getIndexesForTable("rooms")).hasSize(1);
      assertThat(result.getIndexesForTable("users")).hasSize(1);
    }
  }

  @Nested
  class MergeCompositeIndexTest {

    @Test
    void compositeIndexFromJpaIsAdded() {
      IndexMetadata primary =
          new IndexMetadata(
              Map.of("rooms", new ArrayList<>(List.of(idx("rooms", "PRIMARY", "id", 1, false)))));
      IndexMetadata jpa =
          new IndexMetadata(
              Map.of(
                  "rooms",
                  List.of(
                      idx("rooms", "idx_room_location_type", "location_id", 1, true),
                      idx("rooms", "idx_room_location_type", "type", 2, true))));

      IndexMetadata result = primary.merge(jpa);
      List<IndexInfo> roomIndexes = result.getIndexesForTable("rooms");

      // PRIMARY + 2 entries for the composite index
      assertThat(roomIndexes).hasSize(3);
      assertThat(result.hasIndexOn("rooms", "location_id")).isTrue();
      assertThat(result.hasIndexOn("rooms", "type")).isTrue();
    }
  }

  @Nested
  class MergeCaseInsensitiveTest {

    @Test
    void indexNameComparisonIsCaseInsensitive() {
      IndexMetadata primary =
          new IndexMetadata(
              Map.of(
                  "rooms",
                  new ArrayList<>(
                      List.of(idx("rooms", "IDX_ROOM_ARCHIVED", "is_archived", 1, true)))));
      IndexMetadata jpa =
          new IndexMetadata(
              Map.of("rooms", List.of(idx("rooms", "idx_room_archived", "is_archived", 1, true))));

      IndexMetadata result = primary.merge(jpa);
      List<IndexInfo> roomIndexes = result.getIndexesForTable("rooms");

      // Should not duplicate
      assertThat(roomIndexes).hasSize(1);
    }
  }
}
