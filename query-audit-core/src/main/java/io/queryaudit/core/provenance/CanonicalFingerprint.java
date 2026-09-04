package io.queryaudit.core.provenance;

import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/** Typed, length-prefixed encoding prevents delimiter and null/empty collisions. */
final class CanonicalFingerprint {
  private final MessageDigest digest;

  private CanonicalFingerprint() {
    try {
      digest = MessageDigest.getInstance("SHA-256");
    } catch (NoSuchAlgorithmException exception) {
      throw new IllegalStateException("SHA-256 is unavailable", exception);
    }
  }

  static String of(Object value) {
    CanonicalFingerprint fingerprint = new CanonicalFingerprint();
    fingerprint.append(value);
    return HexFormat.of().formatHex(fingerprint.digest.digest());
  }

  private void append(Object value) {
    if (value == null) {
      digest.update((byte) 'N');
    } else if (value instanceof String text) {
      digest.update((byte) 'S');
      size(text.length());
      for (int index = 0; index < text.length(); index++) {
        char unit = text.charAt(index);
        digest.update((byte) (unit >>> 8));
        digest.update((byte) unit);
      }
    } else if (value instanceof Boolean bool) {
      digest.update((byte) (bool ? 'T' : 'F'));
    } else if (value instanceof Integer || value instanceof Long) {
      digest.update((byte) 'I');
      append(value.toString());
    } else if (value instanceof List<?> list) {
      digest.update((byte) 'L');
      size(list.size());
      list.forEach(this::append);
    } else if (value instanceof Map<?, ?> map) {
      TreeMap<String, Object> sorted = new TreeMap<>();
      map.forEach(
          (key, item) -> {
            if (!(key instanceof String text)) {
              throw new IllegalArgumentException("Fingerprint map keys must be strings");
            }
            sorted.put(text, item);
          });
      digest.update((byte) 'M');
      size(sorted.size());
      sorted.forEach(
          (key, item) -> {
            append(key);
            append(item);
          });
    } else {
      throw new IllegalArgumentException(
          "Unsupported fingerprint value: " + value.getClass().getName());
    }
  }

  private void size(int size) {
    digest.update(ByteBuffer.allocate(Integer.BYTES).putInt(size).array());
  }
}
