---
title: Spring Boot 조회 경로 정책
description: Spring Boot 테스트에 쿼리 정책을 걸고, 실패를 확인하고, 감사 결과에서 SQL을 확인해요.
source_digest: 63d332dc4398
---

# Spring Boot 조회 경로 정책 {#spring-boot-read-path-policy}

기존 데이터베이스 테스트로 SELECT 상한과 INSERT/UPDATE/DELETE 0개를 강제해 봐요.
스타터가 Spring `DataSource`를 감싸요. 아래 코드는 가장 최근에 공개된 릴리스를 써요.

## 스타터 추가하기 {#add-the-starter}

[테스트 의존성을 복사](installation.md#spring-boot)하세요. 예산과 개수 계약만 쓴다면 스타터로 충분해요.
인덱스나 EXPLAIN 검사가 필요할 때 데이터베이스 모듈을 추가하세요.

## 통제된 첫 감사 실행하기 {#run-a-controlled-first-audit}

애플리케이션의 테스트 패키지에 `QueryAuditInstallationTest.java`를 만들어요.
아래 import 위에 패키지 선언을 추가하세요.

```java
import static org.junit.jupiter.api.Assertions.assertEquals;

import io.queryaudit.junit5.EnableQueryInspector;
import io.queryaudit.junit5.ExpectQueries;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;

@SpringBootTest
@EnableQueryInspector
class QueryAuditInstallationTest {

    @Autowired
    private JdbcTemplate jdbc;

    @Test
    @ExpectQueries(select = 0, insert = 0, update = 0, delete = 0)
    void capturesOneSelect() {
        assertEquals(1, jdbc.queryForObject("select 1", Integer.class));
    }
}
```

이 테스트만 실행해요.

=== "Gradle"

    ```bash
    ./gradlew test --tests '*QueryAuditInstallationTest'
    ```

=== "Maven"

    ```bash
    mvn -Dtest=QueryAuditInstallationTest test
    ```

**기대하는 실패:**

```text
QueryAudit: capturesOneSelect() exceeded its query budget.
SELECT: executed 1, expected at most 0.
```

`select = 0`을 `select = 1`로 바꾸고 다시 실행하면 **테스트가 통과해야 해요.**
예산 0으로 돌렸는데도 통과하면 [캡처 점검 목록](../guide/troubleshooting.md#queryaudit-not-detecting-any-queries)을 따라가세요.

## 기존 조회 테스트에 정책 적용하기 {#apply-the-policy-to-an-existing-read-test}

기능 단언은 그대로 두고, 다음 어노테이션을 직접 붙여요.

```java
@EnableQueryInspector
@ExpectQueries(select = 1, insert = 0, update = 0, delete = 0)
```

SELECT 상한은 그 서비스나 리포지토리 작업에 맞게 정하세요. 변경 때문에 UPDATE가 추가되면, 기능 단언이
여전히 통과하더라도 정책이 실패해요.

```text
UPDATE: executed 1, expected at most 0.
```

`@EnableQueryInspector`는 탐지 결과를 보고만 하고, 명시한 예산은 여전히 단언으로 동작해요.
통과를 믿기 전에 실제 작업으로 일부러 위반을 만들어 확인하세요.
[실행 가능한 예제](quickstart.md)에서 쓰기 추가와 SELECT 추가 실패를 직접 볼 수 있어요.

!!! note "직접 붙인 어노테이션과 안정적인 컨텍스트"
    설치를 증명할 때는 어노테이션을 테스트에 직접 붙이세요. 합성·상속 어노테이션으로 정책을 공유하거나
    `@DirtiesContext`로 컨텍스트를 교체한다면, 그 설정 그대로 캡처를 확인하세요.
    [보고된 사례와 재현 범위](../guide/limitations.md)를 참고하세요.

## 결과 확인하기 {#inspect-the-result}

활성 테스트 프로필, 예를 들어 `src/test/resources/application.yml`에 다음을 추가해요.

```yaml
query-audit:
  report:
    format: json
```

`build/reports/query-audit/report.json`을 열어요.

| 볼 곳 | 용도 |
| --- | --- |
| `outcome` | 최종 `PASS`, `FAIL`, `INCONCLUSIVE` 판정 확인 |
| `reports[].queries[].sql` | 캡처된 문장 확인 |
| `reports[].queries[].stackTrace` | 애플리케이션 호출 위치 찾기 |

테스트가 여러 개라면 [쿼리 수를 계약 파일에 기록](../guide/contracts.md)하고 의도한 변경은 diff로 리뷰하세요.
[CI에서 완전한 결과를 요구](../guide/first-ci-check.md)하고, [커버리지](../guide/audit-coverage.md)와
[비교 검사](../guide/comparison-inputs.md)로 빠진 테스트나 바뀐 감사 설정이 드러나게 하세요.

## 선택 설정 {#optional-settings}

| 목적 | 설정이나 어노테이션 |
| --- | --- |
| 탐지 결과는 검토만 하고 명시한 예산은 강제 | `@EnableQueryInspector`와 `@ExpectQueries` |
| 설정한 탐지 결과로 실패 | `@QueryAudit` |
| 규칙 묶음 선택 | `query-audit.profile: recommended` |
| 로컬에서 브라우저용 리포트 생성 | `query-audit.report.format: html` |
| 테스트별 setup/teardown SQL도 탐지에 포함 | `@QueryAudit(includeSetupQueries = true)`. 원본 리포트와 예산에는 이미 생명주기 SQL이 포함돼요 |

기본값과 덮어쓰기는 [설정](../guide/configuration.md)을 보세요. JSON/Actions 마스킹은 현재 콘솔과 HTML
진단에는 적용되지 않아요. [리포트 개인정보](../guide/reports.md#machine-report-redaction)를 참고하세요.

## DataSource 캡처 방식 {#how-datasource-capture-works}

스타터는 Spring `DataSource` 빈을 감싸요. JUnit 확장은 실행 중인 테스트의 캡처 리스너를 선택된 쿼리 인식
데이터소스에 연결해요. 애플리케이션 SQL은 감사 구간 동안 바로 그 객체를 거쳐야 해요.

데이터소스가 여러 개라면 감사 대상 코드가 쓰는 것을 보통 `@Primary`로 분명히 하세요. 그렇다고 모든
데이터소스가 하나의 감사로 합쳐지는 건 아니에요. 데이터소스가 여러 개인 애플리케이션에서는 실제
리포지토리 경로로 캡처를 증명하세요.

## 기존 datasource-proxy 재사용하기 {#reuse-an-existing-datasource-proxy}

다른 라이브러리가 이미 쿼리를 인식하는 Spring 데이터소스를 제공한다면, 스타터의 추가 감싸기만 끄세요.

```yaml
query-audit:
  wrap-data-source:
    enabled: false
```

QueryAudit은 켠 상태로 두세요. 확장이 기존 datasource-proxy에 연결되고, 별도 인터셉터 빈은 필요 없어요.
컨텍스트에 원본 데이터소스만 있다면 자동 감싸기를 켠 채로 두세요. 이 설정을 바꾼 뒤에는 SELECT 1개와
예산 0으로 실패하는지 다시 확인하세요.

## 스위트 전체 감사하기 {#audit-a-full-suite}

??? info "첫 테스트가 동작한 뒤 제외 방식 감사 켜기"

    `src/test/resources/junit-platform.properties`에 추가해요.

    ```properties
    junit.jupiter.extensions.autodetection.enabled=true
    ```

    그다음 활성 테스트 YAML에 추가해요.

    ```yaml
    query-audit:
      mode: all
      profile: recommended
    ```

    일부러 감사하지 않을 테스트는 `@QueryAuditExclude`로 제외하세요. 자동 감지만으로는 기본 `annotated`
    모드가 넓어지지 않아요. 지정한 테스트가 실제로 감사 증거를 냈는지 CI가 증명해야 한다면
    [감사 커버리지 목록](../guide/audit-coverage.md)을 쓰세요. 스위트 전체를 필수로 만들기 전에
    [생명주기 한계](../guide/limitations.md)를 확인하세요.

## QueryAudit 끄기 {#disable-queryaudit}

`query-audit.enabled: false`는 감사 없이 실행하려는 경우에만 쓰세요. 잠시 보고만 하고 싶다면 켠 채로
`@EnableQueryInspector`를 쓰세요. 명시한 예산은 여전히 단언으로 동작해요.

## 다음 단계 {#next-steps}

- [테스트별 쿼리 수 변경 리뷰하기](../guide/contracts.md)
- [감사할 테스트를 요구하고 CI 결과 비교하기](../guide/first-ci-check.md)
- [증상별 첫 확인 사항 찾기](../guide/troubleshooting.md)
