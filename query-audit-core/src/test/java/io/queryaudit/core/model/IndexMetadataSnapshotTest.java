package io.queryaudit.core.model;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class IndexMetadataSnapshotTest {

  private static final IndexInfo ID = new IndexInfo("users", "PRIMARY", "id", 1, false, 100);
  private static final IndexInfo TENANT =
      new IndexInfo("users", "tenant_email", "tenant_id", 1, true, 100);
  private static final IndexInfo EMAIL =
      new IndexInfo("users", "tenant_email", "email", 2, true, 100);

  @Test
  void snapshotsBothTheTableMapAndItsIndexLists() {
    List<IndexInfo> indexes = new ArrayList<>(List.of(ID, TENANT, EMAIL));
    Map<String, List<IndexInfo>> tables = new HashMap<>();
    tables.put("users", indexes);
    IndexMetadata metadata = new IndexMetadata(tables);

    indexes.clear();
    tables.clear();
    tables.put("orders", List.of(ID));

    assertThat(metadata.getIndexesForTable("users")).containsExactly(ID, TENANT, EMAIL);
    assertThat(metadata.getIndexesForTable("public.users")).containsExactly(ID, TENANT, EMAIL);
    assertThat(metadata.hasUniqueIndexOn("users", "id")).isTrue();
    assertThat(metadata.hasTable("orders")).isFalse();
  }

  @Test
  void tableAndCompositeLookupsExposeOnlyImmutableCollections() {
    IndexMetadata metadata = new IndexMetadata(Map.of("users", List.of(ID, TENANT, EMAIL)));

    assertThatThrownBy(() -> metadata.getIndexesForTable("users").add(ID))
        .isInstanceOf(UnsupportedOperationException.class);
    assertThatThrownBy(() -> metadata.getIndexesForTable("public.users").clear())
        .isInstanceOf(UnsupportedOperationException.class);
    Map<String, List<IndexInfo>> composite = metadata.getCompositeIndexes("users");
    assertThat(composite).containsOnlyKeys("tenant_email");
    assertThat(composite.get("tenant_email")).containsExactly(TENANT, EMAIL);
    assertThatThrownBy(() -> composite.put("other", List.of(ID)))
        .isInstanceOf(UnsupportedOperationException.class);
    assertThatThrownBy(() -> composite.get("tenant_email").clear())
        .isInstanceOf(UnsupportedOperationException.class);
    assertThatThrownBy(() -> metadata.getCompositeIndexes("missing").put("other", List.of(ID)))
        .isInstanceOf(UnsupportedOperationException.class);
  }

  @Test
  void mergedMetadataRetainsItsOwnImmutableSnapshot() {
    List<IndexInfo> supplementalIndexes = new ArrayList<>(List.of(TENANT, EMAIL));
    IndexMetadata primary = new IndexMetadata(Map.of("users", List.of(ID)));
    IndexMetadata supplemental = new IndexMetadata(Map.of("users", supplementalIndexes));
    IndexMetadata merged = primary.merge(supplemental);

    supplementalIndexes.clear();

    assertThat(primary.getIndexesForTable("users")).containsExactly(ID);
    assertThat(merged.getIndexesForTable("users")).containsExactly(ID, TENANT, EMAIL);
    assertThatThrownBy(() -> merged.getIndexesForTable("users").clear())
        .isInstanceOf(UnsupportedOperationException.class);
  }

  @Test
  void preservesTheDistinctionBetweenUnknownAndKnownEmptyTables() {
    Map<String, List<IndexInfo>> tables = new HashMap<>();
    tables.put(null, List.of(ID));
    tables.put("unknown", null);
    tables.put("empty", List.of());
    tables.put("nullableEntries", Arrays.asList((IndexInfo) null));
    IndexMetadata metadata = new IndexMetadata(tables);

    assertThat(metadata.hasTable(null)).isFalse();
    assertThat(metadata.hasTable("unknown")).isFalse();
    assertThat(metadata.hasTable("empty")).isTrue();
    assertThat(metadata.getIndexesForTable("unknown")).isEmpty();
    assertThat(metadata.getIndexesForTable("empty")).isEmpty();
    assertThat(metadata.getIndexesForTable("nullableEntries")).containsExactly((IndexInfo) null);
  }

  @Test
  void doesNotIntroduceConstructorValidationForLegacyNullMapInput() {
    IndexMetadata metadata = new IndexMetadata(null);

    assertThat(metadata.getIndexesForTable(null)).isEmpty();
    assertThat(metadata.hasTable(null)).isFalse();
    assertThatThrownBy(metadata::isEmpty).isInstanceOf(NullPointerException.class);
  }
}
