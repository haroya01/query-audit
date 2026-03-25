## What

- Added `SqlParser.extractOrBranchColumns()`: splits the WHERE clause on OR and returns the lowercase column name from each branch; returns an empty list when the SQL is unparseable or has no WHERE clause.
- Updated `OrAbuseDetector.evaluate()`: before raising an `OR_ABUSE` issue, checks whether every OR-branched column has an individual index via `IndexMetadata.hasIndexOn()`. If all columns are covered the query is skipped. Falls back to flagging conservatively when `IndexMetadata` contains no data for the table.
- Added `IndexMergeOptimizationTests` nested class to `OrAbuseDetectorTest` with four focused test cases: all columns indexed (not flagged), one unindexed column (flagged), empty index metadata (flagged), and exactly the threshold count with all columns indexed (not flagged).

## Why

When every OR-branched column has its own individual index, MySQL can satisfy the query via `index_merge` (union of range scans) without a full table scan. Previously, `OrAbuseDetector` flagged all multi-column OR patterns regardless of the index state, producing false positives for well-indexed tables.

## Checklist
- [x] `./gradlew build` passes
- [x] Tests added (true positive + false positive)
- [x] False positive test suites still pass
- [x] Commit messages follow conventional commits
