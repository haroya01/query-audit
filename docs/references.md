# References

Query Guard's detection rules are grounded in official database documentation,
peer-reviewed academic research, and established technical literature.

---

## Academic Papers

### SQL Anti-Pattern Detection

| Paper | Venue | Year | Relevance |
|-------|-------|------|-----------|
| Dintyala, P., Narechania, A., and Arulraj, J. **"SQLCheck: Automated Detection and Diagnosis of SQL Anti-Patterns."** | ACM SIGMOD | 2020 | Holistic anti-pattern detection framework with impact ranking and fix suggestions. [(DOI)](https://doi.org/10.1145/3318464.3389754) |
| Lyu, Y., Volokh, S., Halfond, W.G.J., and Tripp, O. **"SAND: A Static Analysis Approach for Detecting SQL Antipatterns."** | ACM ISSTA (Distinguished Paper Award) | 2021 | Static analysis achieving 99.4-100% precision across 1,000 applications. [(DOI)](https://doi.org/10.1145/3460319.3464818) |
| Nagy, C. and Cleve, A. **"SQLInspect: A Static Analyzer to Inspect Database Usage in Java Applications."** | IEEE/ACM ICSE | 2018 | Eclipse plug-in for embedded SQL analysis and code smell detection. [(DOI)](https://doi.org/10.1145/3183440.3183496) |

### Database Performance

| Paper | Venue | Year | Relevance |
|-------|-------|------|-----------|
| Shao, S., Qiu, Z., Yu, X., Yang, W., Jin, G., Xie, T., and Wu, X. **"Database-Access Performance Antipatterns in Database-Backed Web Applications."** | IEEE ICSME | 2020 | Catalogs 34 performance antipatterns (24 known + 10 new) from real-world applications. [(DOI)](https://doi.org/10.1109/ICSME46990.2020.00016) |
| Alshemaimri, B. and Elmasri, R. **"A survey of problematic database code fragments in software systems."** | Engineering Reports | 2021 | Categorization of SQL antipatterns by performance, maintainability, portability, and data integrity. [(DOI)](https://doi.org/10.1002/eng2.12441) |
| Yagoub, K., et al. **"Oracle's SQL Performance Analyzer."** | IEEE Data Engineering Bulletin | 2008 | Query-aware testing by forecasting SQL plan changes across database upgrades. |

---

## Official MySQL Documentation

| Topic | Section | URL |
|-------|---------|-----|
| InnoDB Locking | 17.7.1 | [innodb-locking.html](https://dev.mysql.com/doc/refman/8.0/en/innodb-locking.html) |
| Locks Set by DML Statements | 17.7.3 | [innodb-locks-set.html](https://dev.mysql.com/doc/refman/8.0/en/innodb-locks-set.html) |
| sql_safe_updates | 6.5.1.6 | [mysql-tips.html](https://dev.mysql.com/doc/refman/8.0/en/mysql-tips.html) |
| Optimizing INSERT | 10.2.5.1 | [insert-optimization.html](https://dev.mysql.com/doc/refman/8.0/en/insert-optimization.html) |
| Bulk Data Loading | 10.5.5 | [optimizing-innodb-bulk-data-loading.html](https://dev.mysql.com/doc/refman/8.0/en/optimizing-innodb-bulk-data-loading.html) |
| INSERT ... SELECT | 15.2.7.1 | [insert-select.html](https://dev.mysql.com/doc/refman/8.0/en/insert-select.html) |
| rewriteBatchedStatements | Connector/J | [connector-j-connp-props-performance-extensions.html](https://dev.mysql.com/doc/connectors/en/connector-j-connp-props-performance-extensions.html) |

---

## Official PostgreSQL Documentation

| Topic | URL |
|-------|-----|
| Index Types | [indexes-types.html](https://www.postgresql.org/docs/current/indexes-types.html) |
| Examining Index Usage | [indexes-examine.html](https://www.postgresql.org/docs/current/indexes-examine.html) |
| Performance Tips | [performance-tips.html](https://www.postgresql.org/docs/current/performance-tips.html) |
| EXPLAIN Usage | [using-explain.html](https://www.postgresql.org/docs/current/using-explain.html) |
| Explicit Locking | [explicit-locking.html](https://www.postgresql.org/docs/current/explicit-locking.html) |
| System Catalogs | [catalogs.html](https://www.postgresql.org/docs/current/catalogs.html) |

---

## Books

| Book | Authors | Publisher | Year | ISBN |
|------|---------|-----------|------|------|
| *SQL Antipatterns: Avoiding the Pitfalls of Database Programming* | Karwin, B. | Pragmatic Bookshelf | 2010 | 978-1934356555 |
| *High Performance MySQL*, 3rd ed. | Schwartz, B., Zaitsev, P., Tkachenko, V. | O'Reilly Media | 2012 | 978-1449314286 |
| *High-Performance Java Persistence* | Mihalcea, V. | Leanpub | 2016 | -- |

---

## Detection Rule to Reference Mapping

| Detection Rule | MySQL Docs | PostgreSQL Docs | Papers | Books |
|---------------|-----------|----------------|--------|-------|
| N+1 Query | -- | -- | Shao et al. (2020), Dintyala et al. (2020) | Mihalcea (2016) |
| SELECT * | -- | -- | Alshemaimri & Elmasri (2021), Lyu et al. (2021) | Karwin (2010) |
| WHERE Function | -- | -- | Dintyala et al. (2020) | Schwartz et al. (2012) |
| Missing Index | innodb-locking, innodb-locks-set | indexes-examine | Shao et al. (2020) | Schwartz et al. (2012) |
| UPDATE/DELETE without WHERE | mysql-tips, innodb-locks-set | -- | Dintyala et al. (2020) | Karwin (2010) |
| DML without Index | innodb-locks-set, innodb-locking | explicit-locking | Shao et al. (2020) | Schwartz et al. (2012) |
| Repeated Single INSERT | insert-optimization, bulk-data-loading, Connector/J | -- | -- | Mihalcea (2016) |
| INSERT ... SELECT * | insert-select | -- | Alshemaimri & Elmasri (2021) | Karwin (2010) |
| OFFSET Pagination | -- | -- | Shao et al. (2020) | Schwartz et al. (2012) |
| OR Abuse | -- | -- | Dintyala et al. (2020) | -- |
| FOR UPDATE Locking | innodb-locking | explicit-locking | -- | Schwartz et al. (2012) |
| Cartesian JOIN | -- | -- | Dintyala et al. (2020) | Karwin (2010) |

---

## Related Tools

| Tool | Language | Approach | Comparison with Query Guard |
|------|----------|----------|---------------------------|
| [SQLCheck](https://github.com/jarulraj/sqlcheck) | C++ | Static SQL file analysis | Query Guard analyzes queries at test runtime with index metadata |
| [SAND](https://doi.org/10.1145/3460319.3464818) | Java | Static analysis of source code | Query Guard intercepts actual executed queries |
| [datasource-proxy](https://github.com/ttddyy/datasource-proxy) | Java | Query interception library | Query Guard uses datasource-proxy as its interception layer |
| [p6spy](https://github.com/p6spy/p6spy) | Java | Query logging | Logging only; Query Guard adds analysis and detection |

---

## See Also

- [Architecture Overview](architecture/overview.md) -- How detection rules use these references
- [Configuration Reference](guide/configuration.md) -- All 57 detection rules and their codes
