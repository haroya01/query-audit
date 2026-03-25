# Query Guard - AI Collaboration Guide

## Project Overview
SQL 안티패턴 정적 분석 도구. JUnit 테스트 중 실행되는 SQL을 가로채서 64개 룰로 분석, 성능 문제를 빌드 시점에 잡아냄. MySQL/PostgreSQL 지원.

## Top Priority: False Positive Reduction (오탐지 제거)
- 새 룰 추가보다 **기존 룰의 오탐지 제거**가 최우선
- 변경 시 반드시 FalsePositiveFixTest, AdversarialFalsePositiveTest, OpenSourceCorpusTest 통과 확인
- 오탐지가 의심되면 INFO로 다운그레이드하거나 조건을 좁혀서 해결

## Architecture
- **멀티모듈**: core, junit5, mysql, postgresql, spring-boot-starter
- **핵심 인터페이스**: `DetectionRule.evaluate(List<QueryRecord>, IndexMetadata) -> List<Issue>`
- **Detector 등록**: `QueryAuditAnalyzer.createRules()`에 하드코딩 + ServiceLoader로 외부 확장
- **SQL 파싱**: JSQLParser 5.3 + 자체 `SqlParser` 유틸리티
- **Query 가로채기**: datasource-proxy
- **JUnit 통합**: `QueryAuditExtension` (BeforeAll/BeforeEach/AfterEach/AfterAll)

## Build & Test
```bash
./gradlew build                          # 전체 빌드
./gradlew :query-audit-core:test         # 코어 테스트만
./gradlew :query-audit-core:pitest       # 뮤테이션 테스트 (70% threshold)
```
- Java 17, Gradle, JUnit 5, AssertJ
- Spotless (Google Java Format) - 빌드에서 강제하지 않음

## Coding Conventions
- Detector는 `DetectionRule` 구현, stateless
- `normalizedSql()`로 중복 제거 (LinkedHashSet)
- `SqlParser` 정적 메서드로 SQL 분석
- Issue 생성: IssueType, Severity, query, table, column, detail, suggestion, sourceLocation
- 테스트: true positive + true negative (false positive 방지) 케이스 모두 작성

## False Positive Test Infrastructure
- `FalsePositiveFixTest` - MissingIndex, RedundantFilter 등 실제 오탐 수정 검증
- `AdversarialFalsePositiveTest` - 모든 detector의 엣지케이스 오탐 방지
- `OpenSourceCorpusTest` - PetClinic, JHipster, Keycloak 등 실제 프로젝트 쿼리로 검증
- `Team1FalsePositiveAuditTest` - 200+ 정상 쿼리에 대해 0 오탐 목표
- `RealWorldCorpusBenchmarkTest` - 정밀도 메트릭 측정

## Known Problematic Detectors (오탐지 빈발)
1. **MissingIndexDetector** - soft-delete 컬럼, 저카디널리티 컬럼
2. **RedundantFilterDetector** - JOIN alias 해석, 양방향 OR 패턴
3. **WhereFunctionDetector** - MySQL 8.0 최적화 함수, CTE, expression index
4. **OrAbuseDetector** - index_merge 최적화 미반영

## Key File Locations
- Detector 구현: `query-audit-core/src/main/java/io/queryaudit/core/detector/`
- SQL Parser: `query-audit-core/src/main/java/io/queryaudit/core/parser/SqlParser.java`
- Config: `query-audit-core/src/main/java/io/queryaudit/core/config/QueryAuditConfig.java`
- Analyzer: `query-audit-core/src/main/java/io/queryaudit/core/detector/QueryAuditAnalyzer.java`
- Model (Issue, QueryRecord, IssueType): `query-audit-core/src/main/java/io/queryaudit/core/model/`
