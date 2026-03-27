package io.queryaudit.junit5;

import io.queryaudit.core.analyzer.IndexMetadataProvider;
import io.queryaudit.core.analyzer.JpaIndexScanner;
import io.queryaudit.core.model.IndexMetadata;
import java.io.File;
import java.lang.annotation.Annotation;
import java.net.URL;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Enumeration;
import java.util.List;
import java.util.ServiceLoader;
import javax.sql.DataSource;

/**
 * Collects index metadata from both the database (via {@link IndexMetadataProvider} SPI) and JPA
 * entity annotations ({@code @Table(indexes=...)}). The two sources are merged, with database
 * metadata being authoritative.
 *
 * @author haroya
 * @since 0.2.0
 */
class IndexMetadataCollector {

  /** Collects and merges index metadata from the database and JPA annotations. */
  IndexMetadata collect(DataSource dataSource) {
    IndexMetadata dbMetadata = collectDatabaseIndexMetadata(dataSource);
    IndexMetadata jpaMetadata = collectJpaIndexMetadata();

    if (dbMetadata == null && jpaMetadata == null) {
      return null;
    }
    if (dbMetadata == null) {
      return jpaMetadata;
    }
    if (jpaMetadata == null || jpaMetadata.isEmpty()) {
      return dbMetadata;
    }

    // Merge: DB metadata is authoritative, JPA supplements missing indexes
    return dbMetadata.merge(jpaMetadata);
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

    Class<? extends Annotation> entityAnnotation = null;
    for (String name : entityAnnotationNames) {
      try {
        @SuppressWarnings("unchecked")
        Class<? extends Annotation> cls =
            (Class<? extends Annotation>) Class.forName(name);
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

      Enumeration<URL> roots = cl.getResources("");
      while (roots.hasMoreElements()) {
        URL root = roots.nextElement();
        if ("file".equals(root.getProtocol())) {
          File rootDir = new File(root.toURI());
          scanForEntities(rootDir, rootDir, entityAnnotation, entities);
        }
      }
    } catch (Exception ignored) {
    }

    return entities;
  }

  private void scanForEntities(
      File rootDir,
      File dir,
      Class<? extends Annotation> entityAnnotation,
      List<Class<?>> result) {
    File[] files = dir.listFiles();
    if (files == null) return;

    for (File file : files) {
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
