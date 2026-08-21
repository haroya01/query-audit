package io.queryaudit.core.config;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

@DisplayName("AuditMode (issue #163)")
class AuditModeTest {

  @Nested
  @DisplayName("parse()")
  class Parse {

    @Test
    @DisplayName("'all' parses to ALL, case- and whitespace-insensitive")
    void parsesAll() {
      assertThat(AuditMode.parse("all")).isEqualTo(AuditMode.ALL);
      assertThat(AuditMode.parse("ALL")).isEqualTo(AuditMode.ALL);
      assertThat(AuditMode.parse("  All ")).isEqualTo(AuditMode.ALL);
    }

    @Test
    @DisplayName("'annotated' parses to ANNOTATED")
    void parsesAnnotated() {
      assertThat(AuditMode.parse("annotated")).isEqualTo(AuditMode.ANNOTATED);
      assertThat(AuditMode.parse("ANNOTATED")).isEqualTo(AuditMode.ANNOTATED);
    }

    @Test
    @DisplayName("null and blank keep the pre-0.5.0 default (ANNOTATED)")
    void nullAndBlankDefaultToAnnotated() {
      assertThat(AuditMode.parse(null)).isEqualTo(AuditMode.ANNOTATED);
      assertThat(AuditMode.parse("")).isEqualTo(AuditMode.ANNOTATED);
      assertThat(AuditMode.parse("   ")).isEqualTo(AuditMode.ANNOTATED);
    }

    @Test
    @DisplayName("unknown values fail loudly, naming the accepted ones")
    void unknownValueThrows() {
      assertThatThrownBy(() -> AuditMode.parse("everything"))
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining("everything")
          .hasMessageContaining("annotated")
          .hasMessageContaining("all");
    }
  }

  @Nested
  @DisplayName("QueryAuditConfig wiring")
  class ConfigWiring {

    @Test
    @DisplayName("defaults to ANNOTATED")
    void defaultsToAnnotated() {
      assertThat(QueryAuditConfig.defaults().getAuditMode()).isEqualTo(AuditMode.ANNOTATED);
    }

    @Test
    @DisplayName("builder sets ALL; null keeps the current value")
    void builderSetsAndIgnoresNull() {
      QueryAuditConfig all = QueryAuditConfig.builder().auditMode(AuditMode.ALL).build();
      assertThat(all.getAuditMode()).isEqualTo(AuditMode.ALL);

      QueryAuditConfig nullKept =
          QueryAuditConfig.builder().auditMode(AuditMode.ALL).auditMode(null).build();
      assertThat(nullKept.getAuditMode()).isEqualTo(AuditMode.ALL);
    }

    @Test
    @DisplayName("Builder.from() copies the audit mode")
    void builderFromCopies() {
      QueryAuditConfig source = QueryAuditConfig.builder().auditMode(AuditMode.ALL).build();
      assertThat(QueryAuditConfig.Builder.from(source).build().getAuditMode())
          .isEqualTo(AuditMode.ALL);
    }
  }
}
