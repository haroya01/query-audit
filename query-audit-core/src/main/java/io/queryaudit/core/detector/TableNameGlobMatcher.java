package io.queryaudit.core.detector;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;

/** Matches table names against case-insensitive globs whose only wildcard is {@code *}. */
final class TableNameGlobMatcher {

  static final Set<String> DEFAULT_STAGING_TABLES =
      Set.of("temp_*", "*_temp", "tmp_*", "*_tmp", "staging_*", "*_staging");

  private final List<Pattern> patterns;

  TableNameGlobMatcher(Collection<String> globs) {
    this.patterns = compile(globs);
  }

  boolean matches(String table) {
    if (table == null) {
      return false;
    }
    return patterns.stream().anyMatch(pattern -> pattern.matcher(table).matches());
  }

  private static List<Pattern> compile(Collection<String> globs) {
    if (globs == null || globs.isEmpty()) {
      return List.of();
    }

    List<Pattern> compiled = new ArrayList<>(globs.size());
    for (String glob : globs) {
      if (glob == null || glob.isBlank()) {
        continue;
      }
      compiled.add(Pattern.compile(toRegex(glob), Pattern.CASE_INSENSITIVE));
    }
    return List.copyOf(compiled);
  }

  private static String toRegex(String glob) {
    StringBuilder regex = new StringBuilder("^");
    for (int i = 0; i < glob.length(); i++) {
      char character = glob.charAt(i);
      regex.append(character == '*' ? ".*" : Pattern.quote(String.valueOf(character)));
    }
    return regex.append('$').toString();
  }
}
