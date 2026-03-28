package io.queryaudit.junit5;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

@DisplayName("BooleanOverride")
class BooleanOverrideTest {

  @Test
  @DisplayName("INHERIT.isSpecified() returns false")
  void inherit_isSpecified_returnsFalse() {
    assertThat(BooleanOverride.INHERIT.isSpecified()).isFalse();
  }

  @Test
  @DisplayName("TRUE.isSpecified() returns true")
  void true_isSpecified_returnsTrue() {
    assertThat(BooleanOverride.TRUE.isSpecified()).isTrue();
  }

  @Test
  @DisplayName("FALSE.isSpecified() returns true")
  void false_isSpecified_returnsTrue() {
    assertThat(BooleanOverride.FALSE.isSpecified()).isTrue();
  }

  @Test
  @DisplayName("TRUE.toBoolean() returns true")
  void true_toBoolean_returnsTrue() {
    assertThat(BooleanOverride.TRUE.toBoolean()).isTrue();
  }

  @Test
  @DisplayName("FALSE.toBoolean() returns false")
  void false_toBoolean_returnsFalse() {
    assertThat(BooleanOverride.FALSE.toBoolean()).isFalse();
  }

  @Test
  @DisplayName("INHERIT.toBoolean() throws IllegalStateException")
  void inherit_toBoolean_throwsIllegalStateException() {
    assertThatThrownBy(() -> BooleanOverride.INHERIT.toBoolean())
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("INHERIT");
  }
}
