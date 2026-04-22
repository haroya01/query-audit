package io.queryaudit.core.analyzer;

import io.queryaudit.core.model.IndexInfo;
import io.queryaudit.core.model.IndexMetadata;
import java.io.File;
import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.net.URL;
import java.util.*;

/**
 * Scans the classpath for JPA entity classes and extracts index metadata from
 * {@code @Table(indexes=...)} and {@code @Table(uniqueConstraints=...)} annotations.
 *
 * <p>This supplements database-sourced index metadata, which may be incomplete when running tests
 * against in-memory databases like H2.
 *
 * <p>Handles both {@code jakarta.persistence} and {@code javax.persistence} packages. If JPA
 * annotations are not on the classpath, returns empty metadata.
 *
 * @author haroya
 * @since 0.2.0
 */
public class JpaIndexScanner {

  private static final String[] ENTITY_ANNOTATION_NAMES = {
    "jakarta.persistence.Entity", "javax.persistence.Entity"
  };

  private static final String[] TABLE_ANNOTATION_NAMES = {
    "jakarta.persistence.Table", "javax.persistence.Table"
  };

  /**
   * Scans the given classes for JPA entity annotations and extracts index metadata.
   *
   * @param classes the classes to scan
   * @return IndexMetadata containing indexes declared via JPA annotations
   */
  public IndexMetadata scan(Collection<Class<?>> classes) {
    Map<String, List<IndexInfo>> indexesByTable = new HashMap<>();

    Class<? extends Annotation> entityAnnotation = findAnnotationClass(ENTITY_ANNOTATION_NAMES);
    if (entityAnnotation == null) {
      return new IndexMetadata(Collections.emptyMap());
    }

    Class<? extends Annotation> tableAnnotation = findAnnotationClass(TABLE_ANNOTATION_NAMES);

    for (Class<?> clazz : classes) {
      if (!clazz.isAnnotationPresent(entityAnnotation)) {
        continue;
      }

      String tableName = resolveTableName(clazz, tableAnnotation);
      if (tableName == null || tableName.isEmpty()) {
        continue;
      }

      if (tableAnnotation != null) {
        Annotation tableAnno = clazz.getAnnotation(tableAnnotation);
        if (tableAnno != null) {
          extractIndexes(tableAnno, tableName, indexesByTable);
          extractUniqueConstraints(tableAnno, tableName, indexesByTable);
        }
      }
    }

    return new IndexMetadata(indexesByTable);
  }

  /**
   * Scans entity classes discovered via a classpath scanner. Uses the provided base packages to
   * find entity classes.
   *
   * @param basePackages packages to scan for entities
   * @return IndexMetadata from JPA annotations
   */
  public IndexMetadata scanPackages(String... basePackages) {
    Class<? extends Annotation> entityAnnotation = findAnnotationClass(ENTITY_ANNOTATION_NAMES);
    if (entityAnnotation == null) {
      return new IndexMetadata(Collections.emptyMap());
    }

    List<Class<?>> entityClasses = new ArrayList<>();
    for (String basePackage : basePackages) {
      entityClasses.addAll(findEntityClasses(basePackage, entityAnnotation));
    }

    return scan(entityClasses);
  }

  /**
   * Resolves the table name for an entity class. Uses @Table(name=...) if present, otherwise
   * derives from class name.
   */
  private String resolveTableName(Class<?> clazz, Class<? extends Annotation> tableAnnotation) {
    if (tableAnnotation != null) {
      Annotation tableAnno = clazz.getAnnotation(tableAnnotation);
      if (tableAnno != null) {
        try {
          Method nameMethod = tableAnnotation.getMethod("name");
          String name = (String) nameMethod.invoke(tableAnno);
          if (name != null && !name.isEmpty()) {
            return name.toLowerCase();
          }
        } catch (Exception ignored) {
        }
      }
    }
    // Fallback: derive table name from class name (CamelCase -> snake_case)
    return camelToSnake(clazz.getSimpleName()).toLowerCase();
  }

  /** Extracts @Index entries from @Table(indexes={...}). */
  private void extractIndexes(
      Annotation tableAnno, String tableName, Map<String, List<IndexInfo>> indexesByTable) {
    try {
      Method indexesMethod = tableAnno.annotationType().getMethod("indexes");
      Annotation[] indexes = (Annotation[]) indexesMethod.invoke(tableAnno);

      for (Annotation index : indexes) {
        String indexName = (String) index.annotationType().getMethod("name").invoke(index);
        String columnList = (String) index.annotationType().getMethod("columnList").invoke(index);
        boolean unique = (boolean) index.annotationType().getMethod("unique").invoke(index);

        List<IndexInfo> infos = parseColumnList(tableName, indexName, columnList, !unique);
        indexesByTable.computeIfAbsent(tableName, k -> new ArrayList<>()).addAll(infos);
      }
    } catch (Exception ignored) {
    }
  }

  /** Extracts @UniqueConstraint entries from @Table(uniqueConstraints={...}). */
  private void extractUniqueConstraints(
      Annotation tableAnno, String tableName, Map<String, List<IndexInfo>> indexesByTable) {
    try {
      Method ucMethod = tableAnno.annotationType().getMethod("uniqueConstraints");
      Annotation[] constraints = (Annotation[]) ucMethod.invoke(tableAnno);

      for (Annotation constraint : constraints) {
        String constraintName =
            (String) constraint.annotationType().getMethod("name").invoke(constraint);
        String[] columnNames =
            (String[]) constraint.annotationType().getMethod("columnNames").invoke(constraint);

        if (constraintName == null || constraintName.isEmpty()) {
          constraintName = "uc_" + tableName + "_" + String.join("_", columnNames);
        }

        for (int i = 0; i < columnNames.length; i++) {
          IndexInfo info =
              new IndexInfo(
                  tableName,
                  constraintName,
                  columnNames[i].trim().toLowerCase(),
                  i + 1,
                  false, // unique constraint -> nonUnique = false
                  0);
          indexesByTable.computeIfAbsent(tableName, k -> new ArrayList<>()).add(info);
        }
      }
    } catch (Exception ignored) {
    }
  }

  /** Parses a JPA columnList string (e.g., "location_id, type") into IndexInfo entries. */
  static List<IndexInfo> parseColumnList(
      String tableName, String indexName, String columnList, boolean nonUnique) {
    List<IndexInfo> result = new ArrayList<>();
    if (columnList == null || columnList.isEmpty()) {
      return result;
    }

    String[] columns = columnList.split(",");
    for (int i = 0; i < columns.length; i++) {
      String col = columns[i].trim();
      // Strip ASC/DESC suffixes if present
      String colName = col.split("\\s+")[0].toLowerCase();
      if (!colName.isEmpty()) {
        result.add(new IndexInfo(tableName, indexName, colName, i + 1, nonUnique, 0));
      }
    }
    return result;
  }

  /**
   * Attempts to load a JPA annotation class by name. Returns null if the class is not on the
   * classpath.
   */
  @SuppressWarnings("unchecked")
  private static Class<? extends Annotation> findAnnotationClass(String[] candidateNames) {
    for (String name : candidateNames) {
      try {
        Class<?> clazz = Class.forName(name);
        if (Annotation.class.isAssignableFrom(clazz)) {
          return (Class<? extends Annotation>) clazz;
        }
      } catch (ClassNotFoundException ignored) {
      }
    }
    return null;
  }

  /**
   * Finds entity classes in the given package using classpath scanning. This is a basic
   * implementation using the context class loader.
   */
  private List<Class<?>> findEntityClasses(
      String basePackage, Class<? extends Annotation> entityAnnotation) {
    List<Class<?>> result = new ArrayList<>();
    try {
      String path = basePackage.replace('.', '/');
      ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
      if (classLoader == null) {
        classLoader = getClass().getClassLoader();
      }

      Enumeration<URL> resources = classLoader.getResources(path);
      while (resources.hasMoreElements()) {
        URL resource = resources.nextElement();
        if ("file".equals(resource.getProtocol())) {
          File directory = new File(resource.toURI());
          scanDirectory(directory, basePackage, entityAnnotation, result);
        }
      }
    } catch (Exception ignored) {
    }
    return result;
  }

  private void scanDirectory(
      File directory,
      String packageName,
      Class<? extends Annotation> entityAnnotation,
      List<Class<?>> result) {
    File[] files = directory.listFiles();
    if (files == null) return;

    for (File file : files) {
      if (file.isDirectory()) {
        scanDirectory(file, packageName + "." + file.getName(), entityAnnotation, result);
      } else if (file.getName().endsWith(".class")) {
        String className = packageName + "." + file.getName().replace(".class", "");
        try {
          Class<?> clazz = Class.forName(className);
          if (clazz.isAnnotationPresent(entityAnnotation)) {
            result.add(clazz);
          }
        } catch (Exception | NoClassDefFoundError ignored) {
        }
      }
    }
  }

  /** Converts CamelCase to snake_case. */
  static String camelToSnake(String name) {
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < name.length(); i++) {
      char c = name.charAt(i);
      if (Character.isUpperCase(c)) {
        if (i > 0) {
          sb.append('_');
        }
        sb.append(Character.toLowerCase(c));
      } else {
        sb.append(c);
      }
    }
    return sb.toString();
  }
}
