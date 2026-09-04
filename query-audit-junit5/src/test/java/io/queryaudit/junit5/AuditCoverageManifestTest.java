package io.queryaudit.junit5;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class AuditCoverageManifestTest {

  private static final String ID =
      "[engine:junit-jupiter]/[class:example.OrderTest]/[method:loads()]";

  @TempDir Path directory;

  @Test
  void loadsExactStableIdsWithComments() throws Exception {
    Path manifest = directory.resolve("tests");
    Files.writeString(manifest, "# Audits required in CI\n\n" + ID + "\n");

    assertThat(AuditCoverageManifest.load(manifest)).containsExactly(ID);
  }

  @ParameterizedTest
  @ValueSource(strings = {"", "# No entries", "OrderTest.loads", "[engine:junit-jupiter]"})
  void rejectsEmptyOrMalformedManifest(String content) throws Exception {
    Path manifest = directory.resolve("tests");
    Files.writeString(manifest, content);

    assertThatThrownBy(() -> AuditCoverageManifest.load(manifest)).isInstanceOf(IOException.class);
  }

  @Test
  void rejectsDuplicateIdsWithTheLineNumber() throws Exception {
    Path manifest = directory.resolve("tests");
    Files.writeString(manifest, ID + "\n" + ID + "\n");

    assertThatThrownBy(() -> AuditCoverageManifest.load(manifest))
        .isInstanceOf(IOException.class)
        .hasMessageContaining("line 2");
  }
}
