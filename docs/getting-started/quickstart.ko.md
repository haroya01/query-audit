---
title: 첫 조회 경로 정책
description: 예상하지 못한 쓰기를 막고, 그 SQL과 호출 위치를 확인한 뒤, 같은 JUnit 테스트를 통과시켜요.
source_digest: 4d4d09be8faa
---

# 첫 조회 경로 정책 {#your-first-read-path-policy}

기대한 주문을 돌려주는데도, 조회 경로가 데이터베이스에 쓰기 때문에 실패하는 테스트를 실행해 봐요.
예제는 공개된 QueryAudit `0.6.0`과 인메모리 H2를 써요. **Java 17 이상, Git, 네트워크 연결**이 필요해요.

## 1. 예상하지 못한 쓰기로 실패시키기 {#1-run-the-unexpected-write-failure}

```sh
git clone https://github.com/haroya01/query-audit.git
cd query-audit
./gradlew -p examples/first-audit test -PextraWrite=true --rerun-tasks
```

이 테스트는 SELECT 1개를 허용하고 INSERT, UPDATE, DELETE는 0개만 허용해요.

```java
@ExpectQueries(select = 1, insert = 0, update = 0, delete = 0)
```

주문의 상태는 여전히 기대한 값이지만, 추가된 UPDATE 때문에 정책이 실패해요.

```text
QueryAudit: readsOnce() exceeded its query budget.
UPDATE: executed 1, expected at most 0.
  UPDATE orders SET status = 'NEW' WHERE id = 1
```

이게 기대한 실패예요. 다운로드, 컴파일, 시작 오류는 캡처를 검증한 게 아니에요.
예산 메시지가 보이지 않으면 [문제 해결](../guide/troubleshooting.md)을 참고하세요.

## 2. SQL과 호출 위치 확인하기 {#2-inspect-the-sql-and-call-site}

생성된 감사 결과를 열어요.

```text
examples/first-audit/build/reports/query-audit/report.json
```

| 필드 | 실패한 실행의 결과 |
| --- | --- |
| `outcome` | `FAIL` |
| `reports[0].summary.totalQueries` | `2`: SELECT 1개와 UPDATE 1개 |
| `reports[0].queries`의 UPDATE 항목: `sql` | `UPDATE orders SET status = ? WHERE id = ?` |
| 그 항목의 `stackTrace` | `example.audit.FirstAuditTest.writeOnReadPath:43`로 시작해요 |

JSON은 리터럴 값을 가리고 애플리케이션 호출 위치는 남겨요. 이 릴리스에서는 예산 메시지의 첫 호출
위치가 JDBC 프록시일 수 있어요. 애플리케이션 코드를 찾을 때는 JSON 증거를 쓰세요.

## 3. 쓰기를 빼고 통과시키기 {#3-remove-the-write-and-pass}

같은 테스트를 플래그 없이 실행해요.

```sh
./gradlew -p examples/first-audit test --rerun-tasks
```

```text
[QueryAudit] 1 tests, 1 queries — all clean
[QueryAudit] outcome: PASS
```

새 JSON에는 SELECT 1개만 있고 쓰기는 없어요. 쿼리 정책과 기능 단언이 모두 통과해요.

쿼리 수 회귀도 확인해 보려면:

```sh
./gradlew -p examples/first-audit test -PextraQuery=true --rerun-tasks
```

```text
QueryAudit: readsOnce() exceeded its query budget.
SELECT: executed 2, expected at most 1.
```

실행할 때마다 `report.json`이 바뀌어요. 세 가지 결과를 모두 검증하고 리포트를 남기려면:

```sh
python3 .github/scripts/verify_first_audit.py
```

검증기는 Gradle 종료 코드, JUnit 결과, 캡처된 SQL, 애플리케이션 호출 위치, 공개 아티팩트 버전을
확인해요. 결과는 `examples/first-audit/build/reports/first-audit-verification/`에 저장돼요.

## 4. 내 코드에 예산 적용하기 {#4-apply-the-budget-to-your-code}

| 내 환경 | 다음 단계 |
| --- | --- |
| 기존 Spring Boot 데이터베이스 테스트 | [스타터를 설치](installation.md#spring-boot)하고 [캡처를 검증](spring-boot.md#run-a-controlled-first-audit)해요 |
| 순수 JUnit 5 / JDBC | [계측된 DataSource를 노출](installation.md#plain-junit-5)해요 |

테스트의 기능 단언은 그대로 두세요. 테스트에 `@EnableQueryInspector`와 `@ExpectQueries`를 직접 붙이고,
서비스나 리포지토리가 계측된 DataSource를 쓰게 해요. 그 작업에 맞는 SELECT 상한을 정하고,
INSERT/UPDATE/DELETE를 0으로 두면 조회 경로에 쓰기가 끼어드는 걸 막을 수 있어요.

**예산은 상한이에요.** `select = 1`은 캡처된 SELECT가 0개여도 통과하고, 적지 않은 속성은 검사하지
않아요. 0.7.2부터는 `exact = true`로 적은 속성을 정확한 개수로 바꿀 수 있어요. 통과를 믿기 전에 실제
작업으로 일부러 위반을 만들어 확인하세요. `@EnableQueryInspector`는 탐지 결과를 참고용으로만 두지만,
명시한 예산은 여전히 실패해요.

??? example "실행 가능한 전체 소스"

    픽스처는 감사가 시작되기 전에 준비돼요. 두 플래그는 테스트하는 작업만 바꾸고, 단언과 정책은
    그대로예요.

    ```java
    --8<-- "examples/first-audit/src/test/java/example/audit/FirstAuditTest.java"
    ```

    [Java 소스](https://github.com/haroya01/query-audit/blob/main/examples/first-audit/src/test/java/example/audit/FirstAuditTest.java)
    · [Gradle 빌드](https://github.com/haroya01/query-audit/blob/main/examples/first-audit/build.gradle)

??? info "데이터베이스와 버전 범위"

    H2로는 캡처와 예산을 확인할 수 있어요. 인덱스에 의존하는 검사는 메타데이터가 없다는 안내를 출력할
    수 있어요. 그 데이터베이스의 인덱스를 확인하려면 MySQL이나 PostgreSQL 모듈을 추가하세요.
    예제는 저장소 빌드와 별개로 [공개된 릴리스](versions.md)를 내려받아요.
    스위트 전체를 CI에서 요구하기 전에 [알려진 한계](../guide/limitations.md)를 확인하세요.

## 변경이 생겨도 정책 유지하기 {#keep-the-policy-across-changes}

- [테스트의 쿼리 수를 계약 파일에 기록](../guide/contracts.md)하고, 의도한 변경은 diff로 리뷰해요.
- [실패한 감사를 확인](../guide/reports.md)할 때는 SQL과 호출 위치를 봐요.
- [첫 CI 체크를 추가](../guide/first-ci-check.md)한 뒤 [감사할 테스트를 요구](../guide/audit-coverage.md)하고
  [호환되는 실행을 비교](../guide/comparison-inputs.md)해요.
