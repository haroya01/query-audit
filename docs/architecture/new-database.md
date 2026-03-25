# Adding Database Support

This guide walks through the steps to add support for a new database engine.
We use PostgreSQL as a running example below. Note that **PostgreSQL support is already implemented** in the `query-audit-postgresql` module -- the code shown here reflects the actual implementation and can serve as a reference when adding support for other databases (e.g., MariaDB, Oracle, SQL Server).

## Overview

To support a new database, you need to implement one SPI interface
from `query-audit-core`:

1. **`IndexMetadataProvider`** (required) -- Retrieves index metadata from the database's
   system catalogs.
2. **`ExplainAnalyzer`** (optional) -- Parses the database-specific `EXPLAIN`
   output into `Issue` objects for deeper analysis.

Both are discovered at runtime via **Java ServiceLoader**, so no changes to
`query-audit-core` are required.

---

## Step 1: Create a new module

Add a new Gradle module for the database. For PostgreSQL:

### settings.gradle

```groovy
include 'query-audit-postgresql'
```

### query-audit-postgresql/build.gradle

```groovy
dependencies {
    api project(':query-audit-core')

    testImplementation 'org.junit.jupiter:junit-jupiter:5.11.4'
    testImplementation 'org.testcontainers:postgresql:1.20.4'
    testImplementation 'org.postgresql:postgresql:42.7.4'
}
```

### Package structure

```
query-audit-postgresql/
└── src/
    ├── main/
    │   ├── java/
    │   │   └── io/queryaudit/postgresql/
    │   │       └── PostgreSqlIndexMetadataProvider.java
    │   └── resources/
    │       └── META-INF/
    │           └── services/
    │               └── io.queryaudit.core.analyzer.IndexMetadataProvider
    └── test/
        └── java/
            └── io/queryaudit/postgresql/
                └── PostgreSqlIndexMetadataProviderTest.java
```

---

## Step 2: Implement IndexMetadataProvider

This is the minimum required interface. It reads index information from the
database's system catalogs and returns a unified `IndexMetadata` model.

```java
package io.queryaudit.postgresql;

import io.queryaudit.core.analyzer.IndexMetadataProvider;
import io.queryaudit.core.model.IndexInfo;
import io.queryaudit.core.model.IndexMetadata;

import java.sql.*;
import java.util.*;

public class PostgreSqlIndexMetadataProvider implements IndexMetadataProvider {

    private static final String INDEX_QUERY = """
            SELECT
                t.relname   AS table_name,
                i.relname   AS index_name,
                a.attname   AS column_name,
                array_position(ix.indkey, a.attnum) AS seq_in_index,
                NOT ix.indisunique AS non_unique,
                COALESCE(s.n_distinct, 0) AS cardinality
            FROM pg_class t
            JOIN pg_index ix ON t.oid = ix.indrelid
            JOIN pg_class i  ON i.oid = ix.indexrelid
            JOIN pg_attribute a ON a.attrelid = t.oid
                AND a.attnum = ANY(ix.indkey)
            LEFT JOIN pg_stats s ON s.tablename = t.relname
                AND s.attname = a.attname
            JOIN pg_namespace n ON n.oid = t.relnamespace
            WHERE n.nspname = 'public'
              AND t.relkind = 'r'
            ORDER BY t.relname, i.relname, array_position(ix.indkey, a.attnum)
            """;

    @Override
    public String supportedDatabase() {
        return "postgresql";  // Matched against DatabaseProductName
    }

    @Override
    public IndexMetadata getIndexMetadata(Connection connection) throws SQLException {
        Map<String, List<IndexInfo>> indexesByTable = new HashMap<>();

        try (Statement stmt = connection.createStatement();
             ResultSet rs = stmt.executeQuery(INDEX_QUERY)) {

            while (rs.next()) {
                String tableName  = rs.getString("table_name");
                String indexName  = rs.getString("index_name");
                String columnName = rs.getString("column_name");
                int seqInIndex    = rs.getInt("seq_in_index");
                boolean nonUnique = rs.getBoolean("non_unique");
                long cardinality  = rs.getLong("cardinality");

                IndexInfo info = new IndexInfo(
                    tableName, indexName, columnName,
                    seqInIndex, nonUnique, cardinality
                );

                indexesByTable
                    .computeIfAbsent(tableName, k -> new ArrayList<>())
                    .add(info);
            }
        }

        return new IndexMetadata(indexesByTable);
    }
}
```

### Key points

- **`supportedDatabase()`** must return a string that appears in the JDBC
  `DatabaseMetaData.getDatabaseProductName()`. For PostgreSQL, the driver returns
  `"PostgreSQL"`, so returning `"postgresql"` (lowercase) works because the
  matching in `QueryAuditExtension` uses `contains()` on the lowercased product
  name.
- The `IndexInfo` record expects: table name, index name, column name,
  sequence position in the index, whether the index is non-unique, and
  cardinality.

---

## Step 3: Register via ServiceLoader

Create the service descriptor file:

**`src/main/resources/META-INF/services/io.queryaudit.core.analyzer.IndexMetadataProvider`**

```
io.queryaudit.postgresql.PostgreSqlIndexMetadataProvider
```

That is the entire file -- one line with the fully qualified class name.

When `query-audit-postgresql` is on the classpath, `ServiceLoader` will automatically
discover the provider. No configuration or annotation is needed.

---

## Step 4: Implement ExplainAnalyzer (optional)

!!! note
    This step is optional. The `ExplainAnalyzer` interface is designed for
    EXPLAIN-based analysis that can detect additional issues beyond SQL parsing
    and index metadata. You can skip this step and still get full detection
    from the 57 SQL-parsing and index-metadata rules.

When ready, implement the `ExplainAnalyzer` interface:

```java
package io.queryaudit.postgresql;

import io.queryaudit.core.analyzer.ExplainAnalyzer;
import io.queryaudit.core.model.Issue;
import io.queryaudit.core.model.QueryRecord;

import java.util.List;

public class PostgreSqlExplainAnalyzer implements ExplainAnalyzer {

    @Override
    public String supportedDatabase() {
        return "postgresql";
    }

    @Override
    public List<Issue> analyze(String explainOutput, QueryRecord query) {
        // Parse PostgreSQL EXPLAIN (FORMAT JSON) output
        // Look for:
        //   - Seq Scan nodes (full table scan)
        //   - Sort nodes without index (filesort equivalent)
        //   - Materialize nodes (temporary table equivalent)
        //   - Nested Loop with inner Seq Scan (inefficient join)

        // Return a list of Issue objects for each detected problem.
        return List.of();
    }
}
```

Register it in a second service descriptor file:

**`src/main/resources/META-INF/services/io.queryaudit.core.analyzer.ExplainAnalyzer`**

```
io.queryaudit.postgresql.PostgreSqlExplainAnalyzer
```

### PostgreSQL EXPLAIN format considerations

PostgreSQL supports several EXPLAIN output formats:

| Format | Command | Notes |
|---|---|---|
| Text | `EXPLAIN query` | Human-readable, harder to parse |
| JSON | `EXPLAIN (FORMAT JSON) query` | Structured, recommended for parsing |
| YAML | `EXPLAIN (FORMAT YAML) query` | Structured alternative |
| XML | `EXPLAIN (FORMAT XML) query` | Structured alternative |

The JSON format is recommended for implementation because it provides a structured
tree of plan nodes that can be parsed with any JSON library.

---

## Step 5: Add tests

Use Testcontainers to run integration tests against a real database:

```java
package io.queryaudit.postgresql;

import io.queryaudit.core.model.IndexMetadata;
import org.junit.jupiter.api.*;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

@Testcontainers
class PostgreSqlIndexMetadataProviderTest {

    @Container
    static PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:16");

    private Connection connection;

    @BeforeEach
    void setUp() throws SQLException {
        connection = DriverManager.getConnection(
            postgres.getJdbcUrl(),
            postgres.getUsername(),
            postgres.getPassword()
        );

        try (Statement stmt = connection.createStatement()) {
            stmt.execute("""
                CREATE TABLE IF NOT EXISTS orders (
                    id SERIAL PRIMARY KEY,
                    user_id INT NOT NULL,
                    status VARCHAR(50),
                    created_at TIMESTAMP DEFAULT NOW()
                )
                """);
            stmt.execute("CREATE INDEX IF NOT EXISTS idx_orders_user_id ON orders (user_id)");
        }
    }

    @AfterEach
    void tearDown() throws SQLException {
        if (connection != null) {
            connection.close();
        }
    }

    @Test
    void shouldCollectIndexMetadata() throws SQLException {
        PostgreSqlIndexMetadataProvider provider = new PostgreSqlIndexMetadataProvider();

        assertEquals("postgresql", provider.supportedDatabase());

        IndexMetadata metadata = provider.getIndexMetadata(connection);
        assertNotNull(metadata);
        assertNotNull(metadata.getIndexesForTable("orders"));
        assertFalse(metadata.getIndexesForTable("orders").isEmpty());
    }

    @Test
    void shouldDetectUserIdIndex() throws SQLException {
        PostgreSqlIndexMetadataProvider provider = new PostgreSqlIndexMetadataProvider();
        IndexMetadata metadata = provider.getIndexMetadata(connection);

        boolean hasUserIdIndex = metadata.getIndexesForTable("orders").stream()
            .anyMatch(idx -> "user_id".equals(idx.columnName()));
        assertTrue(hasUserIdIndex, "Should detect index on orders.user_id");
    }
}
```

---

## Checklist

Use this checklist when adding a new database:

- [ ] Create module directory: `query-audit-<database>/`
- [ ] Add `build.gradle` with dependency on `query-audit-core`
- [ ] Add module to `settings.gradle`
- [ ] Implement `IndexMetadataProvider`
    - [ ] `supportedDatabase()` returns lowercase database product name
    - [ ] `getIndexMetadata()` queries system catalogs for index information
    - [ ] Returns `IndexMetadata` with all indexes grouped by table
- [ ] Create `META-INF/services/io.queryaudit.core.analyzer.IndexMetadataProvider`
- [ ] (Optional) Implement `ExplainAnalyzer`
- [ ] (Optional) Create `META-INF/services/io.queryaudit.core.analyzer.ExplainAnalyzer`
- [ ] Add Testcontainers-based integration tests
    - [ ] Test that `supportedDatabase()` returns the expected value
    - [ ] Test that indexes are collected from tables with explicit indexes
    - [ ] Test that primary key indexes are collected
    - [ ] Test that composite indexes report correct column ordering
    - [ ] Test behavior with empty database (no tables)
- [ ] Verify `./gradlew test` passes
- [ ] Update project documentation (supported databases list)

---

## See Also

- [Architecture Overview](overview.md) -- Module structure and IndexMetadataProvider SPI
- [Contributing Guide](contributing.md) -- General contribution workflow
- [Configuration Reference](../guide/configuration.md) -- Database module classpath configuration
