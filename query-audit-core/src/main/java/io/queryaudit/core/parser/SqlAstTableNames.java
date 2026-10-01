package io.queryaudit.core.parser;

import static io.queryaudit.core.parser.SqlAstParser.parse;

import java.util.ArrayList;
import java.util.List;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.delete.Delete;
import net.sf.jsqlparser.statement.insert.Insert;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.update.Update;
import net.sf.jsqlparser.util.TablesNamesFinder;

/** AST table discovery, excluding INSERT targets and preserving outer-table-first order. */
final class SqlAstTableNames {
  private SqlAstTableNames() {}

  static List<String> extractTableNames(String sql) throws Exception {
    // Mirrors the regex baseline: only FROM / JOIN / UPDATE / DELETE targets, not INSERT.
    Statement statement = parse(sql);

    List<String> result = new ArrayList<>();
    String outer = outermostTable(statement);
    if (outer != null) {
      addTable(outer, result);
    }

    if (statement instanceof Insert ins && ins.getSelect() == null) {
      return result;
    }

    TablesNamesFinder<?> finder = new TablesNamesFinder<>();
    for (String raw : finder.getTableList(statement)) {
      if (statement instanceof Insert insSel
          && insSel.getTable() != null
          && stripQualifier(raw).equals(stripQualifier(insSel.getTable().getName()))) {
        continue;
      }
      addTable(raw, result);
    }
    return result;
  }

  static String stripQualifier(String raw) {
    if (raw == null) return "";
    String s = raw.replace("`", "").replace("\"", "");
    int dot = s.lastIndexOf('.');
    return dot >= 0 ? s.substring(dot + 1) : s;
  }

  static String outermostTable(Statement stmt) {
    if (stmt instanceof Select sel) {
      PlainSelect ps = sel.getPlainSelect();
      if (ps != null && ps.getFromItem() instanceof Table t) {
        return t.getName();
      }
    } else if (stmt instanceof Delete del && del.getTable() != null) {
      return del.getTable().getName();
    } else if (stmt instanceof Update upd && upd.getTable() != null) {
      return upd.getTable().getName();
    }
    return null;
  }

  static void addTable(String raw, List<String> out) {
    if (raw == null) return;
    String cleaned = raw.replace("`", "").replace("\"", "");
    if (cleaned.contains(".")) {
      cleaned = cleaned.substring(cleaned.lastIndexOf('.') + 1);
    }
    if (!cleaned.isEmpty() && !out.contains(cleaned)) {
      out.add(cleaned);
    }
  }
}
