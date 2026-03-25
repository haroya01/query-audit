package io.queryaudit.core.detector;

import io.queryaudit.core.interceptor.LazyLoadTracker;
import io.queryaudit.core.model.Issue;
import io.queryaudit.core.model.IssueType;
import io.queryaudit.core.model.Severity;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Detects N+1 problems by analyzing Hibernate lazy load records.
 *
 * <p>This detector is the authoritative source for N+1 detection. It analyzes {@link
 * LazyLoadTracker.LazyLoadRecord} entries grouped by collection role (for
 * {@code @OneToMany}/{@code @ManyToMany}) or by proxy entity class (for
 * {@code @ManyToOne}/{@code @OneToOne}).
 *
 * <p>Two types of records are handled:
 *
 * <ul>
 *   <li><b>Collection records</b> -- role like {@code "com.example.Order.items"}. If the same
 *       collection role is initialized for {@code threshold}+ different owner entity IDs, that is a
 *       confirmed N+1.
 *   <li><b>Proxy records</b> -- role like {@code "proxy:com.example.User"}. If the same entity
 *       class is loaded via proxy resolution for {@code threshold}+ different entity IDs, that is a
 *       confirmed @ManyToOne/@OneToOne N+1.
 * </ul>
 *
 * <p>Unlike SQL-level heuristics, this approach has zero false positives because it only tracks
 * actual Hibernate lazy loading events, not explicit queries.
 *
 * @author haroya
 * @since 0.2.0
 */
public class LazyLoadNPlusOneDetector {

  private final int threshold;

  public LazyLoadNPlusOneDetector(int threshold) {
    this.threshold = threshold;
  }

  public LazyLoadNPlusOneDetector() {
    this(3);
  }

  /**
   * Evaluates lazy load records for N+1 patterns.
   *
   * @param records the lazy load records collected during a test method
   * @return list of confirmed N+1 issues
   */
  public List<Issue> evaluate(List<LazyLoadTracker.LazyLoadRecord> records) {
    List<Issue> issues = new ArrayList<>();

    // Group by collection role (or proxy entity class)
    Map<String, List<LazyLoadTracker.LazyLoadRecord>> byRole =
        records.stream()
            .collect(Collectors.groupingBy(LazyLoadTracker.LazyLoadRecord::collectionRole));

    for (var entry : byRole.entrySet()) {
      List<LazyLoadTracker.LazyLoadRecord> group = entry.getValue();
      if (group.size() < threshold) continue;

      // Count unique owner IDs -- if same role loaded for N different owners, it's N+1
      long uniqueOwnerIds =
          group.stream().map(LazyLoadTracker.LazyLoadRecord::ownerIdString).distinct().count();

      if (uniqueOwnerIds < threshold) continue; // Same entity reloaded, not N+1

      String role = entry.getKey();
      boolean isProxy = role.startsWith(LazyLoadTracker.PROXY_ROLE_PREFIX);

      Issue issue =
          isProxy
              ? buildProxyIssue(role, group, uniqueOwnerIds)
              : buildCollectionIssue(role, group, uniqueOwnerIds);

      issues.add(issue);
    }

    return issues;
  }

  /** Builds an issue for @OneToMany/@ManyToMany collection N+1. */
  private Issue buildCollectionIssue(
      String role, List<LazyLoadTracker.LazyLoadRecord> group, long uniqueOwnerIds) {
    String ownerEntity = group.get(0).ownerEntity();

    String collectionName = role.contains(".") ? role.substring(role.lastIndexOf('.') + 1) : role;
    String entityName = simpleClassName(ownerEntity);

    String detail =
        String.format(
            "Lazy collection '%s' on %s initialized %d times for %d different entities",
            collectionName, entityName, group.size(), uniqueOwnerIds);

    String suggestion =
        String.format(
            "1) Add @EntityGraph(attributePaths = {\"%s\"}) to your repository method\n"
                + "         2) Use JOIN FETCH in JPQL: SELECT e FROM %s e JOIN FETCH e.%s\n"
                + "         3) Add @BatchSize(size=%d) on the %s collection field",
            collectionName,
            entityName,
            collectionName,
            Math.min((int) (uniqueOwnerIds * 2), 100),
            collectionName);

    return new Issue(
        IssueType.N_PLUS_ONE,
        Severity.ERROR,
        "Lazy load: " + role,
        extractTableName(role),
        null,
        detail,
        suggestion,
        null);
  }

  /** Builds an issue for @ManyToOne/@OneToOne proxy N+1. */
  private Issue buildProxyIssue(
      String role, List<LazyLoadTracker.LazyLoadRecord> group, long uniqueOwnerIds) {
    // role = "proxy:com.example.User"
    String fqcn = role.substring(LazyLoadTracker.PROXY_ROLE_PREFIX.length());
    String entityName = simpleClassName(fqcn);

    String detail =
        String.format(
            "Lazy @ManyToOne/@OneToOne proxy '%s' loaded %d times for %d different IDs",
            entityName, group.size(), uniqueOwnerIds);

    String suggestion =
        String.format(
            "1) Use @EntityGraph to eagerly fetch the association that references %s\n"
                + "         2) Use JOIN FETCH in JPQL: SELECT e FROM ParentEntity e JOIN FETCH e.%s\n"
                + "         3) Add @BatchSize(size=%d) on the @ManyToOne/%s field in the parent entity",
            entityName,
            Character.toLowerCase(entityName.charAt(0)) + entityName.substring(1),
            Math.min((int) (uniqueOwnerIds * 2), 100),
            entityName);

    return new Issue(
        IssueType.N_PLUS_ONE,
        Severity.ERROR,
        "Lazy load: " + role,
        entityName,
        null,
        detail,
        suggestion,
        null);
  }

  private String simpleClassName(String fqcn) {
    return fqcn.contains(".") ? fqcn.substring(fqcn.lastIndexOf('.') + 1) : fqcn;
  }

  private String extractTableName(String role) {
    // "com.example.Order.items" -> "items" (approximation)
    if (role.contains(".")) {
      return role.substring(role.lastIndexOf('.') + 1);
    }
    return role;
  }
}
