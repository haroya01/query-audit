package io.queryaudit.junit5.identity.alpha;

import io.queryaudit.junit5.IdentityCaptureExtension;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

@ExtendWith(IdentityCaptureExtension.class)
public class SameNameFixture {

  @Test
  @DisplayName("duplicate")
  void work() {}

  @ParameterizedTest(name = "duplicate")
  @ValueSource(strings = {"first", "second"})
  void work(String value) {}

  @Nested
  class NestedGroup {

    @Test
    @DisplayName("duplicate")
    void work() {}
  }
}
