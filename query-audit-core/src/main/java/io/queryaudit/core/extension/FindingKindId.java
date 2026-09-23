package io.queryaudit.core.extension;

import io.queryaudit.core.model.IssueType;
import java.util.Arrays;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * A policy code for a kind of finding, not the identity of an individual report occurrence. Custom
 * kinds use namespaced IDs. Existing built-in issue codes retain their exact spelling.
 */
public record FindingKindId(String value) {
  private static final Set<String> BUILTIN_CODES =
      Arrays.stream(IssueType.values())
          .map(IssueType::getCode)
          .collect(Collectors.toUnmodifiableSet());

  public FindingKindId {
    if (!isBuiltinCode(value)) {
      ExtensionIds.requireNamespaced(value, "Finding kind ID");
    }
  }

  public static FindingKindId of(String value) {
    return new FindingKindId(value);
  }

  public static FindingKindId builtin(IssueType type) {
    return new FindingKindId(Objects.requireNonNull(type, "type").getCode());
  }

  public boolean isBuiltin() {
    return isBuiltinCode(value);
  }

  private static boolean isBuiltinCode(String value) {
    return value != null && BUILTIN_CODES.contains(value);
  }

  @Override
  public String toString() {
    return value;
  }
}
