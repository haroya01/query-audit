package io.queryaudit.junit5;

import io.queryaudit.core.analyzer.IndexMetadataProvider;
import io.queryaudit.core.analyzer.JpaIndexScanner;
import io.queryaudit.core.model.IndexMetadata;
import io.queryaudit.core.provenance.AuditCapability;
import io.queryaudit.core.provenance.AuditRuntimeIdentity;
import java.io.File;
import java.lang.annotation.Annotation;
import java.net.URL;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;
import java.util.Locale;
import java.util.ServiceConfigurationError;
import java.util.ServiceLoader;
import javax.sql.DataSource;

/**
 * Collects index metadata from the database (via {@link IndexMetadataProvider} SPI), falling back
 * to JPA entity annotations ({@code @Table(indexes=...)}) only when no database provider is
 * available (e.g., H2 or other unsupported databases).
 *
 * <p>When database metadata is successfully collected, it is used exclusively. JPA annotations are
 * not merged in, because they may declare indexes that do not exist in the actual database schema
 * (e.g., when {@code ddl-auto=none} and migrations have not been applied). Merging JPA-only indexes
 * would cause false positives in index-related detectors.
 *
 * @author haroya
 * @since 0.2.0
 * @see <a href="https://github.com/haroya01/query-guard/issues/43">#43</a>
 */
class IndexMetadataCollector {

  record Result(
      IndexMetadata metadata, String dialect, AuditCapability capability, String failure) {}

  private final Iterable<IndexMetadataProvider> providers;

  IndexMetadataCollector() {
    this(ServiceLoader.load(IndexMetadataProvider.class));
  }

  IndexMetadataCollector(Iterable<IndexMetadataProvider> providers) {
    this.providers = providers;
  }

  IndexMetadata collect(DataSource dataSource) {
    return collectWithCapabilities(dataSource).metadata();
  }

  Result collectWithCapabilities(DataSource dataSource) {
    String dialect = null;
    String source = "jdbc-metadata";
    try (Connection connection = dataSource.getConnection()) {
      String product = connection.getMetaData().getDatabaseProductName();
      if (product == null || product.isBlank()) {
        throw new IllegalStateException("JDBC metadata did not identify the database product");
      }
      dialect = product.toLowerCase(Locale.ROOT);
      for (IndexMetadataProvider provider : providers) {
        if (dialect.contains(provider.supportedDatabase().toLowerCase(Locale.ROOT))) {
          source =
              AuditRuntimeIdentity.hasKnownCapabilityInputs(provider.getClass())
                  ? AuditRuntimeIdentity.implementation(provider.getClass())
                  : AuditRuntimeIdentity.unverifiedImplementation(provider.getClass());
          IndexMetadata metadata = provider.getIndexMetadata(connection);
          if (metadata == null) {
            throw new IllegalStateException("Index metadata provider returned null");
          }
          return new Result(
              metadata,
              dialect,
              AuditCapability.available(
                  source, AuditRuntimeIdentity.hasKnownCapabilityInputs(provider.getClass())),
              null);
        }
      }
    } catch (Exception | LinkageError | ServiceConfigurationError failure) {
      return failed(dialect, source, failure);
    }

    source = "jpa-annotations";
    try {
      List<Class<?>> entities = discoverEntityClasses();
      if (entities.isEmpty()) {
        return new Result(null, dialect, AuditCapability.absent(), null);
      }
      source = AuditRuntimeIdentity.implementation(JpaIndexScanner.class);
      IndexMetadata metadata = new JpaIndexScanner().scan(entities);
      return new Result(
          metadata.isEmpty() ? null : metadata, dialect, AuditCapability.available(source), null);
    } catch (Exception | LinkageError failure) {
      return failed(dialect, source, failure);
    }
  }

  private static Result failed(String dialect, String source, Throwable failure) {
    return new Result(
        null, dialect, AuditCapability.failed(source), failure.getClass().getSimpleName());
  }

  /**
   * Discovers JPA entity classes from the classpath. Tries Spring's EntityManagerFactory first,
   * then falls back to classpath scanning.
   */
  private List<Class<?>> discoverEntityClasses() {
    return discoverEntitiesFromClasspath();
  }

  private List<Class<?>> discoverEntitiesFromClasspath() {
    List<Class<?>> entities = new ArrayList<>();
    String[] entityAnnotationNames = {"jakarta.persistence.Entity", "javax.persistence.Entity"};

    Class<? extends Annotation> entityAnnotation = null;
    for (String name : entityAnnotationNames) {
      try {
        @SuppressWarnings("unchecked")
        Class<? extends Annotation> cls = (Class<? extends Annotation>) Class.forName(name);
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
    } catch (Exception failure) {
      throw new IllegalStateException("Could not discover JPA index metadata", failure);
    }

    return entities;
  }

  private void scanForEntities(
      File rootDir, File dir, Class<? extends Annotation> entityAnnotation, List<Class<?>> result) {
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
