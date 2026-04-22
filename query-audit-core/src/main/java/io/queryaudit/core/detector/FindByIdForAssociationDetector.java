package io.queryaudit.core.detector;

import io.queryaudit.core.interceptor.LazyLoadTracker;
import io.queryaudit.core.interceptor.LazyLoadTracker.ExplicitLoadRecord;
import io.queryaudit.core.interceptor.LazyLoadTracker.LazyLoadRecord;
import io.queryaudit.core.model.Issue;
import io.queryaudit.core.model.IssueType;
import io.queryaudit.core.model.QueryRecord;
import io.queryaudit.core.model.Severity;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * Detects cases where {@code findById()} is used only to set a FK association, where {@code
 * getReferenceById()} (proxy-only, no SELECT) would suffice.
 *
 * <p>The detection cross-references three data sources:
 *
 * <ol>
 *   <li>{@link LazyLoadTracker#getExplicitLoads()} — explicit PK loads via {@code findById}
 *   <li>{@link LazyLoadTracker#getRecords()} — lazy init records (proxy/collection resolution)
 *   <li>{@link QueryRecord} list — subsequent INSERT/UPDATE SQL referencing the entity
 * </ol>
 *
 * <p>An explicit load is flagged when:
 *
 * <ul>
 *   <li>The entity was loaded via {@code findById} (present in ExplicitLoadRecord)
 *   <li>No lazy collection or proxy was subsequently initialized for that entity type+id
 *   <li>A subsequent INSERT or UPDATE SQL references a column that could be a FK to the loaded
 *       entity
 * </ul>
 *
 * <p>Severity is {@link Severity#INFO} because eager basic field access (e.g., {@code
 * user.getName()}) cannot be detected at runtime, so false positives remain possible.
 *
 * @author haroya
 * @since 0.4.0
 */
public class FindByIdForAssociationDetector {

  /**
   * Evaluates explicit load records for findById-for-association patterns.
   *
   * @param explicitLoads explicit PK loads recorded by LazyLoadTracker
   * @param lazyLoadRecords lazy load records (proxy/collection) for cross-reference
   * @param queries SQL queries captured during the test
   * @return list of INFO-level issues for findById-for-association candidates
   */
  public List<Issue> evaluate(
      List<ExplicitLoadRecord> explicitLoads,
      List<LazyLoadRecord> lazyLoadRecords,
      List<QueryRecord> queries) {

    if (explicitLoads == null || explicitLoads.isEmpty()) {
      return List.of();
    }

    // Build set of entity type+id pairs that had lazy init (proxy or collection resolution).
    // If an entity was lazily accessed, findById was justified (the entity's data was used).
    Set<String> lazyInitializedEntities =
        lazyLoadRecords.stream()
            .map(FindByIdForAssociationDetector::lazyLoadKey)
            .collect(Collectors.toSet());

    // Collect INSERT/UPDATE queries that occurred after each explicit load.
    // Deduplicate by entity type+id to avoid reporting the same entity twice.
    List<Issue> issues = new ArrayList<>();
    Set<String> reported = new HashSet<>();

    for (ExplicitLoadRecord load : explicitLoads) {
      String entityKey = load.entityType() + "#" + load.idString();

      // Skip duplicates (same entity loaded multiple times via findById)
      if (!reported.add(entityKey)) {
        continue;
      }

      // Skip if the entity had any lazy init → its data was used beyond FK
      if (lazyInitializedEntities.contains(entityKey)) {
        continue;
      }

      // Check if a subsequent INSERT/UPDATE references this entity type as a FK
      String simpleEntityName = simpleClassName(load.entityType());
      boolean hasSubsequentDml =
          hasSubsequentFkReference(queries, load.timestamp(), simpleEntityName);

      if (hasSubsequentDml) {
        issues.add(buildIssue(load, simpleEntityName));
      }
    }

    return issues;
  }

  /**
   * Checks if any INSERT/UPDATE query after the given timestamp could reference the entity as a FK.
   * Looks for column names like {@code entity_id}, {@code entityId}, or table names matching the
   * entity. Uses word boundary matching to avoid false positives (e.g., "abuser_id" should not
   * match "user_id").
   */
  private boolean hasSubsequentFkReference(
      List<QueryRecord> queries, long afterTimestamp, String simpleEntityName) {

    // Build word-boundary patterns for FK column matching
    String snakeFk = toSnakeCase(simpleEntityName) + "_id";
    String camelFk =
        Character.toLowerCase(simpleEntityName.charAt(0)) + simpleEntityName.substring(1) + "Id";

    // Word boundary: preceded by non-alphanumeric or start of string
    Pattern snakePattern =
        Pattern.compile(
            "(?<![a-zA-Z0-9_])" + Pattern.quote(snakeFk) + "(?![a-zA-Z0-9_])",
            Pattern.CASE_INSENSITIVE);
    Pattern camelPattern =
        Pattern.compile("(?<![a-zA-Z0-9_])" + Pattern.quote(camelFk) + "(?![a-zA-Z0-9_])");

    for (QueryRecord query : queries) {
      if (query.timestamp() <= afterTimestamp) continue;

      String sqlUpper = query.sql().toUpperCase(Locale.ROOT);
      if (!sqlUpper.contains("INSERT") && !sqlUpper.contains("UPDATE")) continue;

      // Check for FK column reference with word boundary
      if (snakePattern.matcher(query.sql()).find()) return true;
      if (camelPattern.matcher(query.sql()).find()) return true;
    }

    return false;
  }

  private Issue buildIssue(ExplicitLoadRecord load, String simpleEntityName) {
    String detail =
        String.format(
            "findById() loaded '%s' (id=%s) but no field access was detected. "
                + "If this entity is only used to set a FK association, "
                + "getReferenceById() avoids the unnecessary SELECT",
            simpleEntityName, load.idString());

    String suggestion =
        String.format(
            "Replace repository.findById(%s).orElseThrow() with "
                + "repository.getReferenceById(%s) when the entity is only used for "
                + "setting a @ManyToOne/@OneToOne association (FK assignment)",
            load.idString(), load.idString());

    return new Issue(
        IssueType.FIND_BY_ID_FOR_ASSOCIATION,
        Severity.INFO,
        "findById: " + load.entityType() + "#" + load.idString(),
        simpleEntityName,
        null,
        detail,
        suggestion,
        load.stackTrace());
  }

  /**
   * Creates a key for a lazy load record to match against explicit load records. For proxy records,
   * extracts the entity type from the role prefix. For collection records, uses the owner entity.
   */
  private static String lazyLoadKey(LazyLoadRecord record) {
    if (record.collectionRole().startsWith(LazyLoadTracker.PROXY_ROLE_PREFIX)) {
      // proxy:com.example.User → entity type is the resolved entity
      String entityType =
          record.collectionRole().substring(LazyLoadTracker.PROXY_ROLE_PREFIX.length());
      return entityType + "#" + record.ownerIdString();
    }
    // Collection init: owner entity had its collection accessed → owner was "used"
    return record.ownerEntity() + "#" + record.ownerIdString();
  }

  private static String simpleClassName(String fqcn) {
    return fqcn.contains(".") ? fqcn.substring(fqcn.lastIndexOf('.') + 1) : fqcn;
  }

  /** Converts "UserProfile" to "user_profile". */
  private static String toSnakeCase(String camelCase) {
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < camelCase.length(); i++) {
      char c = camelCase.charAt(i);
      if (Character.isUpperCase(c)) {
        if (i > 0) sb.append('_');
        sb.append(Character.toLowerCase(c));
      } else {
        sb.append(c);
      }
    }
    return sb.toString();
  }
}
