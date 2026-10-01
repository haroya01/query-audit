package io.queryaudit.core.extension;

final class ExtensionIds {
  private ExtensionIds() {}

  static void requireNamespaced(String value, String label) {
    if (value == null
        || value.length() > 200
        || !value.matches("[a-z][a-z0-9.-]*:[a-z][a-z0-9._/-]*")) {
      throw new IllegalArgumentException(
          label
              + " must be a lowercase namespaced ID such as 'acme:query-budget' (max 200"
              + " characters)");
    }
  }
}
