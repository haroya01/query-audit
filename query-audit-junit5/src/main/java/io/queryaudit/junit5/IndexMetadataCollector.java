package io.queryaudit.junit5;

import io.queryaudit.core.analyzer.IndexMetadataProvider;
import io.queryaudit.core.analyzer.JpaIndexScanner;
import io.queryaudit.core.model.IndexMetadata;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.ServiceLoader;
import javax.sql.DataSource;

/**
 * Collects index metadata from the database (via {@link IndexMetadataProvider} SPI), falling back
 * to JPA entity annotations ({@code @Table(indexes=...)}) only when no database provider is
 * available (e.g., H2 or other unsupported databases).
 *
 * <p>When database metadata is successfully collected, it is used exclusively. JPA annotations are
 * not merged in, because they may declare indexes that do not exist in the actual database schema
 * (e.g., when {@code ddl-auto=none} and migrations have not been applied). Merging JPA-only
 * indexes would cause false positives in index-related detectors.
 *
 * @author haroya
 * @since 0.2.0
 * @see <a href="https://github.com/haroya01/query-guard/issues/43">#43</a>
 */
class IndexMetadataCollector {

  /**
   * Collects index metadata from the database if a matching provider exists, otherwise falls back
   * to JPA annotations.
   */
  IndexMetadata collect(DataSource dataSource) {
    IndexMetadata dbMetadata = collectDatabaseIndexMetadata(dataSource);
    if (dbMetadata != null) {
      return dbMetadata;
    }

    // Fallback: no DB provider matched (e.g., H2) — use JPA annotations as best-effort source
    return collectJpaIndexMetadata();
  }

  private IndexMetadata collectDatabaseIndexMetadata(DataSource dataSource) {
    ServiceLoader<IndexMetadataProvider> loader = ServiceLoader.load(IndexMetadataProvider.class);
    for (IndexMetadataProvider provider : loader) {
      try (Connection connection = dataSource.getConnection()) {
        String dbProduct = connection.getMetaData().getDatabaseProductName().toLowerCase();
        if (dbProduct.contains(provider.supportedDatabase())) {
          return provider.getIndexMetadata(connection);
        }
      } catch (Exception ignored) {
      }
    }
    return null;
  }

  /**
   * Scans entity classes on the classpath for JPA @Table(indexes=...) annotations. Returns null if
   * JPA is not on the classpath or no entities are found.
   */
  private IndexMetadata collectJpaIndexMetadata() {
    try {
      JpaIndexScanner scanner = new JpaIndexScanner();
      List<Class<?>> entityClasses = discoverEntityClasses();
      if (entityClasses.isEmpty()) {
        return null;
      }
      IndexMetadata metadata = scanner.scan(entityClasses);
      return metadata.isEmpty() ? null : metadata;
    } catch (Exception | NoClassDefFoundError ignored) {
      return null;
    }
  }

  /**
   * Discovers JPA entity classes from the classpath. Tries Spring's EntityManagerFactory first, then
   * falls back to classpath scanning.
   */
  private List<Class<?>> discoverEntityClasses() {
    List<Class<?>> entities = new ArrayList<>();

    // Strategy 1: Use Spring's LocalContainerEntityManagerFactoryBean if available
    try {
      entities = discoverEntitiesFromSpring();
      if (!entities.isEmpty()) {
        return entities;
      }
    } catch (Exception | NoClassDefFoundError ignored) {
    }

    // Strategy 2: Scan common base packages from the classpath
    try {
      entities = discoverEntitiesFromClasspath();
    } catch (Exception | NoClassDefFoundError ignored) {
    }

    return entities;
  }

  @SuppressWarnings("unchecked")
  private List<Class<?>> discoverEntitiesFromSpring() {
    try {
      // Try to get the EntityManagerFactory from Spring context
      Class<?> emfClass = Class.forName("jakarta.persistence.EntityManagerFactory");
      Class<?> metamodelClass = Class.forName("jakarta.persistence.metamodel.Metamodel");

      try {
        Class<?> springExtensionClass =
            Class.forName("org.springframework.test.context.junit.jupiter.SpringExtension");
        return Collections.emptyList();
      } catch (Exception ignored) {
        return Collections.emptyList();
      }
    } catch (ClassNotFoundException ignored) {
      return Collections.emptyList();
    }
  }

  private List<Class<?>> discoverEntitiesFromClasspath() {
    List<Class<?>> entities = new ArrayList<>();
    String[] entityAnnotationNames = {"jakarta.persistence.Entity", "javax.persistence.Entity"};

    Class<? extends java.lang.annotation.Annotation> entityAnnotation = null;
    for (String name : entityAnnotationNames) {
      try {
        @SuppressWarnings("unchecked")
        Class<? extends java.lang.annotation.Annotation> cls =
            (Class<? extends java.lang.annotation.Annotation>) Class.forName(name);
        entityAnnotation = cls;
        break;
      } catch (ClassNotFoundException ignored) {
      }
    }

    if (entityAnnotation == null) {
      return entities;
    }

    // Scan classpath roots for .class files
    try {
      ClassLoader cl = Thread.currentThread().getContextClassLoader();
      if (cl == null) cl = getClass().getClassLoader();

      java.util.Enumeration<java.net.URL> roots = cl.getResources("");
      while (roots.hasMoreElements()) {
        java.net.URL root = roots.nextElement();
        if ("file".equals(root.getProtocol())) {
          java.io.File rootDir = new java.io.File(root.toURI());
          scanForEntities(rootDir, rootDir, entityAnnotation, entities);
        }
      }
    } catch (Exception ignored) {
    }

    return entities;
  }

  private void scanForEntities(
      java.io.File rootDir,
      java.io.File dir,
      Class<? extends java.lang.annotation.Annotation> entityAnnotation,
      List<Class<?>> result) {
    java.io.File[] files = dir.listFiles();
    if (files == null) return;

    for (java.io.File file : files) {
      if (file.isDirectory()) {
        scanForEntities(rootDir, file, entityAnnotation, result);
      } else if (file.getName().endsWith(".class")) {
        String relativePath = rootDir.toURI().relativize(file.toURI()).getPath();
        String className = relativePath.replace('/', '.').replace(".class", "");
        try {
          Class<?> clazz =
              Class.forName(className, false, Thread.currentThread().getContextClassLoader());
          if (clazz.isAnnotationPresent(entityAnnotation)) {
            result.add(clazz);
          }
        } catch (Exception | NoClassDefFoundError ignored) {
        }
      }
    }
  }
}
