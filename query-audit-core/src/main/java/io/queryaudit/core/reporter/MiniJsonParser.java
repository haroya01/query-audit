package io.queryaudit.core.reporter;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Minimal recursive-descent JSON parser for reading back {@code report.json} in the compare
 * command. Exists because core is deliberately dependency-free — the same reason the writer side
 * ({@link JsonReporter}) is {@code StringBuilder}-based.
 *
 * <p>Parses the full JSON grammar (RFC 8259): objects as {@link LinkedHashMap}, arrays as {@link
 * ArrayList}, strings with all escape sequences, numbers as {@link Long} when integral and {@link
 * Double} otherwise, booleans, and {@code null}. Not tuned for adversarial input — the input is
 * QueryAudit's own reporter output.
 *
 * @author haroya
 * @since 0.5.0
 */
final class MiniJsonParser {

  private final String input;
  private int pos;

  private MiniJsonParser(String input) {
    this.input = input;
  }

  /** Parses a complete JSON document; trailing non-whitespace is an error. */
  static Object parse(String json) {
    MiniJsonParser parser = new MiniJsonParser(json);
    parser.skipWhitespace();
    Object value = parser.parseValue();
    parser.skipWhitespace();
    if (parser.pos != json.length()) {
      throw parser.error("trailing content");
    }
    return value;
  }

  private Object parseValue() {
    char c = peek();
    return switch (c) {
      case '{' -> parseObject();
      case '[' -> parseArray();
      case '"' -> parseString();
      case 't', 'f' -> parseBoolean();
      case 'n' -> parseNull();
      default -> parseNumber();
    };
  }

  private Map<String, Object> parseObject() {
    expect('{');
    Map<String, Object> result = new LinkedHashMap<>();
    skipWhitespace();
    if (peek() == '}') {
      pos++;
      return result;
    }
    while (true) {
      skipWhitespace();
      String key = parseString();
      skipWhitespace();
      expect(':');
      skipWhitespace();
      result.put(key, parseValue());
      skipWhitespace();
      char c = next();
      if (c == '}') {
        return result;
      }
      if (c != ',') {
        throw error("expected ',' or '}' in object");
      }
    }
  }

  private List<Object> parseArray() {
    expect('[');
    List<Object> result = new ArrayList<>();
    skipWhitespace();
    if (peek() == ']') {
      pos++;
      return result;
    }
    while (true) {
      skipWhitespace();
      result.add(parseValue());
      skipWhitespace();
      char c = next();
      if (c == ']') {
        return result;
      }
      if (c != ',') {
        throw error("expected ',' or ']' in array");
      }
    }
  }

  private String parseString() {
    expect('"');
    StringBuilder sb = new StringBuilder();
    while (true) {
      char c = next();
      if (c == '"') {
        return sb.toString();
      }
      if (c == '\\') {
        char esc = next();
        switch (esc) {
          case '"' -> sb.append('"');
          case '\\' -> sb.append('\\');
          case '/' -> sb.append('/');
          case 'b' -> sb.append('\b');
          case 'f' -> sb.append('\f');
          case 'n' -> sb.append('\n');
          case 'r' -> sb.append('\r');
          case 't' -> sb.append('\t');
          case 'u' -> {
            if (pos + 4 > input.length()) {
              throw error("truncated \\u escape");
            }
            sb.append((char) Integer.parseInt(input.substring(pos, pos + 4), 16));
            pos += 4;
          }
          default -> throw error("invalid escape '\\" + esc + "'");
        }
      } else {
        sb.append(c);
      }
    }
  }

  private Boolean parseBoolean() {
    if (input.startsWith("true", pos)) {
      pos += 4;
      return Boolean.TRUE;
    }
    if (input.startsWith("false", pos)) {
      pos += 5;
      return Boolean.FALSE;
    }
    throw error("invalid literal");
  }

  private Object parseNull() {
    if (input.startsWith("null", pos)) {
      pos += 4;
      return null;
    }
    throw error("invalid literal");
  }

  private Object parseNumber() {
    int start = pos;
    if (peek() == '-') {
      pos++;
    }
    boolean integral = true;
    while (pos < input.length()) {
      char c = input.charAt(pos);
      if (c >= '0' && c <= '9') {
        pos++;
      } else if (c == '.' || c == 'e' || c == 'E' || c == '+' || c == '-') {
        integral = false;
        pos++;
      } else {
        break;
      }
    }
    if (pos == start) {
      throw error("expected a value");
    }
    String number = input.substring(start, pos);
    try {
      return integral ? (Object) Long.parseLong(number) : (Object) Double.parseDouble(number);
    } catch (NumberFormatException e) {
      throw error("invalid number '" + number + "'");
    }
  }

  private char peek() {
    if (pos >= input.length()) {
      throw error("unexpected end of input");
    }
    return input.charAt(pos);
  }

  private char next() {
    char c = peek();
    pos++;
    return c;
  }

  private void expect(char expected) {
    char c = next();
    if (c != expected) {
      throw error("expected '" + expected + "' but found '" + c + "'");
    }
  }

  private void skipWhitespace() {
    while (pos < input.length()) {
      char c = input.charAt(pos);
      if (c == ' ' || c == '\t' || c == '\n' || c == '\r') {
        pos++;
      } else {
        break;
      }
    }
  }

  private IllegalArgumentException error(String message) {
    return new IllegalArgumentException("Malformed JSON at offset " + pos + ": " + message);
  }
}
