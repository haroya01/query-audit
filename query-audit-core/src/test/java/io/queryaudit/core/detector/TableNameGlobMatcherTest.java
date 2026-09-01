package io.queryaudit.core.detector;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Arrays;
import java.util.List;
import org.junit.jupiter.api.Test;

class TableNameGlobMatcherTest {

  @Test
  void treatsOnlyAsteriskAsAWildcard() {
    TableNameGlobMatcher matcher = new TableNameGlobMatcher(List.of("audit.v1+_*"));

    assertThat(matcher.matches("AUDIT.V1+_EVENTS")).isTrue();
    assertThat(matcher.matches("auditXv11_events")).isFalse();
  }

  @Test
  void ignoresNullAndBlankGlobs() {
    TableNameGlobMatcher matcher = new TableNameGlobMatcher(Arrays.asList(null, "", "  "));

    assertThat(matcher.matches("anything")).isFalse();
    assertThat(matcher.matches(null)).isFalse();
  }
}
