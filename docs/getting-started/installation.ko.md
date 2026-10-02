---
title: 설치
description: 테스트 의존성을 추가하고, 조회 경로 정책을 걸고, 일부러 만든 실패로 동작을 확인해요.
source_digest: c11ae9b2841f
---

# 설치 {#installation}

| 시작점 | 사용할 것 |
| --- | --- |
| 기존 Spring Boot 데이터베이스 테스트 | [Spring Boot 스타터](#spring-boot) |
| Spring 없는 JUnit 5 | [순수 JUnit 5](#plain-junit-5) |
| 바로 실행해 볼 예제 | [예상하지 못한 쓰기로 실패시키고 통과시키기](quickstart.md) |

QueryAudit은 **테스트 클래스패스**에 추가해요. 아래 코드는 가장 최근에 공개된 릴리스를 써요.
지원 범위와 검증한 조합은 [버전](versions.md)에 정리돼 있어요.

## Spring Boot {#spring-boot}

기존 데이터베이스 테스트에 스타터를 추가해요. 스타터가 Spring `DataSource`를 자동으로 감싸요.
JDBC 드라이버, 연결 설정, 마이그레이션, 픽스처는 그대로 두세요.

=== "Gradle · Kotlin"

    ```kotlin
    dependencies {
        testImplementation("org.springframework.boot:spring-boot-starter-test")
        testImplementation("io.github.haroya01:query-audit-spring-boot-starter:0.7.2") // x-release-please-version
        testImplementation("io.github.haroya01:query-audit-mysql:0.7.2") // x-release-please-version
    }

    tasks.test {
        useJUnitPlatform()
    }
    ```

=== "Gradle · Groovy"

    ```groovy
    dependencies {
        testImplementation 'org.springframework.boot:spring-boot-starter-test'
        testImplementation 'io.github.haroya01:query-audit-spring-boot-starter:0.7.2' // x-release-please-version
        testImplementation 'io.github.haroya01:query-audit-mysql:0.7.2' // x-release-please-version
    }

    test {
        useJUnitPlatform()
    }
    ```

=== "Maven"

    ```xml
    <dependencies>
        <dependency>
            <groupId>org.springframework.boot</groupId>
            <artifactId>spring-boot-starter-test</artifactId>
            <scope>test</scope>
        </dependency>
        <dependency>
            <groupId>io.github.haroya01</groupId>
            <artifactId>query-audit-spring-boot-starter</artifactId>
            <version>0.7.2</version> <!-- x-release-please-version -->
            <scope>test</scope>
        </dependency>
        <dependency>
            <groupId>io.github.haroya01</groupId>
            <artifactId>query-audit-mysql</artifactId>
            <version>0.7.2</version> <!-- x-release-please-version -->
            <scope>test</scope>
        </dependency>
    </dependencies>
    ```

`spring-boot-starter-test`는 Spring Boot가 관리하는 버전을 그대로 쓰세요. 이미 있다면 필요한 QueryAudit
의존성만 추가하면 돼요.

| 테스트 데이터베이스 | 사용할 데이터베이스 모듈 |
| --- | --- |
| MySQL | 위처럼 `query-audit-mysql` |
| PostgreSQL | `query-audit-mysql` 대신 `query-audit-postgresql` |
| 쿼리 예산과 개수 계약만 쓸 때 | 데이터베이스 모듈 없이 스타터만. 스타터에 JUnit 연동이 들어 있어요 |

**실행해 보기:** [SELECT 예산 0으로 실패시킨 뒤 조회 경로 정책 적용하기](spring-boot.md#run-a-controlled-first-audit)

## 순수 JUnit 5 {#plain-junit-5}

Java 17 이상의 JUnit 5 테스트에서 캡처, 예산, 개수 계약을 쓰려면 `query-audit-junit5`를 써요.
H2 의존성은 [실행 가능한 예제](quickstart.md)용이에요. 실제로는 쓰고 있는 데이터베이스를 쓰세요.

=== "Gradle · Kotlin"

    ```kotlin
    dependencies {
        testImplementation("org.junit.jupiter:junit-jupiter:5.11.4")
        testRuntimeOnly("org.junit.platform:junit-platform-launcher:1.11.4")
        testImplementation("io.github.haroya01:query-audit-junit5:0.7.2") // x-release-please-version
        testImplementation("net.ttddyy:datasource-proxy:1.10")
        testImplementation("com.h2database:h2:2.3.232")
    }

    tasks.test {
        useJUnitPlatform()
    }
    ```

=== "Gradle · Groovy"

    ```groovy
    dependencies {
        testImplementation 'org.junit.jupiter:junit-jupiter:5.11.4'
        testRuntimeOnly 'org.junit.platform:junit-platform-launcher:1.11.4'
        testImplementation 'io.github.haroya01:query-audit-junit5:0.7.2' // x-release-please-version
        testImplementation 'net.ttddyy:datasource-proxy:1.10'
        testImplementation 'com.h2database:h2:2.3.232'
    }

    test {
        useJUnitPlatform()
    }
    ```

=== "Maven"

    ```xml
    <dependencies>
        <dependency>
            <groupId>org.junit.jupiter</groupId>
            <artifactId>junit-jupiter</artifactId>
            <version>5.11.4</version>
            <scope>test</scope>
        </dependency>
        <dependency>
            <groupId>io.github.haroya01</groupId>
            <artifactId>query-audit-junit5</artifactId>
            <version>0.7.2</version> <!-- x-release-please-version -->
            <scope>test</scope>
        </dependency>
        <dependency>
            <groupId>net.ttddyy</groupId>
            <artifactId>datasource-proxy</artifactId>
            <version>1.10</version>
            <scope>test</scope>
        </dependency>
        <dependency>
            <groupId>com.h2database</groupId>
            <artifactId>h2</artifactId>
            <version>2.3.232</version>
            <scope>test</scope>
        </dependency>
    </dependencies>
    ```

프로젝트가 이미 JUnit 버전을 관리한다면 호환되는 JUnit 버전과 launcher 버전을 맞춰 쓰세요.
Maven은 JUnit 5 테스트를 실행하도록 이미 설정돼 있어야 해요. 테스트를 하나도 찾지 못한 빌드는 캡처를
검증한 게 아니에요.

`ProxyDataSource`를 static 테스트 필드로 노출하고, 테스트 대상 코드가 **바로 그 객체**를 쓰게 하세요.
[전체 테스트와 설정](quickstart.md#4-apply-the-budget-to-your-code)을 복사해서 쓰면 돼요.

기존 MySQL이나 PostgreSQL 테스트라면 `query-audit-junit5` 대신 `query-audit-mysql`이나
`query-audit-postgresql`을 쓰세요. 두 모듈 모두 JUnit 연동을 포함해요. 명시적인 프록시 설정을 위해
`datasource-proxy`는 그대로 두고, H2 대신 쓰고 있는 드라이버와 픽스처를 쓰세요.

??? info "QueryAudit이 수정 가능한 필드를 감싸게 하기 (0.6.0 이상)"

    확장은 `static javax.sql.DataSource`로 선언한 원본 필드를 테스트 동안 기록용 프록시로 바꿀 수 있어요.
    이 필드는 수정 가능해야 해요. 원본 `static final` 필드나 구체적인 커넥션 풀 타입은 이 방식으로 바꿀 수
    없어요. 리포지토리는 감싼 뒤에 현재 필드 값으로 만들어야 해요. 그 전에 원본 데이터소스로 만든
    리포지토리는 캡처를 우회해요.

    빠른 시작의 명시적인 static 프록시는 이런 교체 조건이 필요 없고, 이전 릴리스에서도 그대로 쓸 수 있는
    방식이에요.

## 실패시킨 뒤 조회 정책 유지하기 {#run-a-failure-then-keep-the-read-policy}

SELECT를 하나 실행하는 기존 테스트에 `@EnableQueryInspector`와 이 정책을 직접 붙여요.

```java
@ExpectQueries(select = 0, insert = 0, update = 0, delete = 0)
```

실행해서 다음 실패가 나오는지 확인해요.

```text
SELECT: executed 1, expected at most 0.
```

그다음 `select = 1`로 바꾸고 다시 실행해요. 테스트가 통과해야 하고, INSERT/UPDATE/DELETE 예산 0은 그
조회 경로에 쓰기가 추가되는 걸 계속 잡아요. 기능 단언은 그대로 두세요.

예산은 상한이라서, 테스트가 통과했다는 것만으로 SQL이 캡처됐다고 볼 수는 없어요.
예산 0으로 돌렸는데도 통과하면 [캡처 누락 문제 해결](../guide/troubleshooting.md#queryaudit-not-detecting-any-queries)을 따라가세요.

다음은 [테스트별 개수 계약 기록](../guide/contracts.md)이나 [CI에서 감사 요구하기](../guide/first-ci-check.md)예요.

## 모듈 고르기 {#module-selection}

| 모듈 | 용도 |
| --- | --- |
| `query-audit-spring-boot-starter` | Spring DataSource 자동 감싸기와 설정 바인딩 |
| `query-audit-junit5` | 순수 JUnit 캡처, 예산, 개수 계약, 데이터베이스와 무관한 검사 |
| `query-audit-mysql` | JUnit 연동 + MySQL 인덱스 메타데이터와 EXPLAIN |
| `query-audit-postgresql` | JUnit 연동 + PostgreSQL 인덱스 메타데이터와 EXPLAIN |
| `query-audit-core` | JUnit 없이 쓰는 분석, 모델, 리포터, 비교 |

스타터와 데이터베이스 모듈은 `query-audit-junit5`와 `query-audit-core`를 전이 의존성으로 포함해요.
그 모듈의 공개 API를 코드에서 직접 쓸 때만 직접 의존성을 추가하세요.

## 호환성 {#compatibility}

**Java 17 이상과 JUnit 5**를 쓰세요. 공개 릴리스의 범위는 [검증한 조합](versions.md#tested-combinations)과
[알려진 한계](../guide/limitations.md)에 있어요.

### SQL 파서 의존성 {#sql-parser-dependency}

0.6.0부터 JSqlParser 5.3이 필수 전이 의존성이에요. 제외하지 마세요. 파서가 없거나, 호환되지 않거나,
버전을 확인할 수 없으면 초기화가 실패해요.

??? info "파서 버전 덮어쓰기와 대체 경로"

    구조 추출은 먼저 JSqlParser를 써요. 지원하지 않는 문장과 10,000자보다 긴 문장은 그 문장에 한해 내장
    대체 경로를 써요. 단순 패턴 검사와 정규화도 내장 파싱을 써요. 대체 경로로 처리됐다고 그 SQL을 완전히
    지원한다는 뜻은 아니에요.

    의존성 관리가 JSqlParser 버전을 덮어쓴다면 `EnhancedSqlParser.parserVersion()`을 확인하세요.
    다시 패키징한 JAR는 Maven 버전 메타데이터를 유지해야 해요.
    [파서 문제 해결](../guide/troubleshooting.md#sql-is-too-complex-for-the-parser)을 참고하세요.

## 다음 단계 {#next-step}

[첫 예산을 검증](quickstart.md)한 뒤 [첫 CI 체크를 추가](../guide/first-ci-check.md)하세요.
