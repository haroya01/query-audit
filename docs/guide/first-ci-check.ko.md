---
title: 첫 CI 체크
description: 예상하지 못한 조회와 쓰기에서 CI를 실패시키고, 그 실행의 SQL 증거를 남겨요.
source_digest: 38703c4bf28f
---

# 첫 CI 체크 {#your-first-ci-check}

조회·쓰기 예산 테스트 하나를 실행하고, 그 SQL 증거를 남기고, 테스트 명령의 성공과 `outcome: PASS`인 새
JSON 리포트를 함께 요구해요.

| 실행 결과 | CI 결과 |
|---|---|
| 조회·쓰기 개수가 예산 안이고 감사가 완료됨 | 통과 |
| 금지한 쓰기가 실행되거나 SELECT 상한을 넘음 | 실패. 테스트 로그에서 SQL과 캡처된 호출 위치를 확인해요 |
| 캡처가 불완전하거나, 리포트가 없거나, 테스트 명령이 실패함 | 실패. 완전한 실행을 되살린 뒤에 받아들여요 |

이 가이드는 애플리케이션에 이미 데이터베이스를 쓰는 테스트 잡과 [테스트 의존성](../getting-started/installation.md)이
있다고 가정해요. JSON 판정 게이트는 [버전과 호환성](../getting-started/versions.md)에 설명된 리포트 형식이
필요해요. `outcome`이 없는 예전 리포트는 이 체크에서 실패해요.

## 1. 작은 테스트 하나로 캡처 확인하기 {#1-verify-capture-with-one-small-test}

이 클래스를 애플리케이션 패키지의 `src/test/java` 아래에 두고, import 위에 패키지 선언을 추가하세요.
기존 Spring Boot 애플리케이션과 테스트 데이터베이스를 쓰고, 엔티티나 새 테이블은 필요 없어요.

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
class QueryBudgetTest {

    @Autowired
    JdbcTemplate jdbc;

    @Test
    @ExpectQueries(select = 1, insert = 0, update = 0, delete = 0)
    void oneReadDoesNotWrite() {
        assertEquals(1, jdbc.queryForObject("SELECT 1", Integer.class));
    }
}
```

이 예산은 SELECT를 최대 1개까지 허용하고 INSERT, UPDATE, DELETE는 허용하지 않아요.
`@EnableQueryInspector`는 탐지 결과를 참고용으로만 두고, 명시한 예산은 넘으면 여전히 실패해요. 콘솔을 한
번 보고 이 테스트가 `SELECT 1`을 캡처했는지 확인하세요.

예산이 실제로 동작하는지 보려면, 같은 SELECT를 잠시 한 번 더 실행해 보세요. 테스트가 다음 진단과 함께
실패해야 해요(일부):

```text
QueryAudit: oneReadDoesNotWrite() exceeded its query budget.
SELECT: executed 2, expected at most 1.
```

테스트를 남기기 전에 추가한 호출은 지우세요.

설정을 확인한 뒤에는 `jdbc.queryForObject(...)`를 실제 서비스나 리포지토리 호출로 바꾸고 결과를 단언하세요.
그 작업에 맞는 예산을 정하세요. 이 스모크 테스트만으로는 애플리케이션 동작을 지켜 주지 못해요. JPA 쓰기를
확인할 때는 테스트 트랜잭션 안에서 대기 중인 변경을 flush해서, SQL이 감사하는 테스트 본문 안에서
실행되게 하세요.

## 2. 테스트 프로세스에 JSON 요청하기 {#2-ask-the-test-process-for-json}

Spring Boot라면 `src/test/resources/application-query-audit-ci.yml`을 만들어요.

```yaml
query-audit:
  enabled: true
  fail-on-detection: false
  auto-open-report: false
  report:
    format: json
    output-dir: build/reports/query-audit
```

`SPRING_PROFILES_ACTIVE=query-audit-ci`로 켜요. 데이터베이스 설정이 다른 프로필에 있다면 둘 다 넣으세요.
예를 들면 `SPRING_PROFILES_ACTIVE=test,query-audit-ci`예요. 아래 스크립트는 환경에 이미 설정된 프로필 뒤에
`query-audit-ci`를 붙여요. 기존 CI의 데이터베이스 자격 증명과 서비스 설정은 그대로 두세요.

??? info "순수 JUnit: Gradle 테스트 JVM에 설정 넘기기"
    위의 Spring 테스트 대신 [순수 JUnit 캡처 설정](../getting-started/installation.md#plain-junit-5)을 쓰세요.
    `./gradlew test -DqueryAudit.report.format=json` 같은 명령은 Gradle이 따로 띄우는 테스트 JVM에 속성을
    자동으로 넘기지 않아요. `test` 태스크를 설정하세요.

    === "Kotlin DSL"

        ```kotlin
        tasks.named<Test>("test") {
            systemProperty("queryAudit.report.format", "json")
            systemProperty("queryAudit.report.outputDir", "build/reports/query-audit")
            systemProperty("queryAudit.autoOpenReport", "false")
        }
        ```

    === "Groovy DSL"

        ```groovy
        tasks.named('test', Test) {
            systemProperty 'queryAudit.report.format', 'json'
            systemProperty 'queryAudit.report.outputDir', 'build/reports/query-audit'
            systemProperty 'queryAudit.autoOpenReport', 'false'
        }
        ```

    이렇게 설정했다면 아래 스크립트에서 Spring 프로필 변수를 빼고, 테스트 필터의 `QueryBudgetTest`를 실제
    클래스 이름(빠른 시작이라면 `FirstAuditTest`)으로 바꾸세요. 명령줄로 고르는 설정과 Maven은
    [CI 빌드 도구 설정](ci-cd.md#plain-junit-build-tool-setup)을 참고하세요.

## 3. 테스트를 실행하고 산출물 확인하기 {#3-run-the-test-and-check-the-artifact}

다음을 `ci/query-audit.sh`로 저장하고 애플리케이션 모듈 디렉터리에서 실행하세요. Bash와 Python 3이
필요해요. 리포트 경로는 위에서 설정한 출력 디렉터리와 같아야 해요.

```bash
#!/usr/bin/env bash
set -euo pipefail

# A previous successful report must not satisfy this run's gate.
rm -f build/reports/query-audit/report.json

test_exit=0
SPRING_PROFILES_ACTIVE="${SPRING_PROFILES_ACTIVE:+${SPRING_PROFILES_ACTIVE},}query-audit-ci" \
  ./gradlew test --tests '*QueryBudgetTest' --rerun-tasks --no-build-cache \
  || test_exit=$?

TEST_EXIT="$test_exit" python3 - <<'PY'
import json
import os
import sys
from pathlib import Path

path = Path("build/reports/query-audit/report.json")
if not path.is_file():
    sys.exit("QueryAudit failed: this run did not write report.json")

report = json.loads(path.read_text(encoding="utf-8"))
if not isinstance(report, dict):
    sys.exit("QueryAudit failed: expected a JSON report envelope")

errors = []
if os.environ["TEST_EXIT"] != "0":
    errors.append("the test command failed")
if report.get("outcome") != "PASS":
    errors.append(f"audit outcome is {report.get('outcome')!r}")
if not isinstance(report.get("reports"), list) or not report["reports"]:
    errors.append("no audited test results were reported")
if errors:
    sys.exit("QueryAudit failed: " + "; ".join(errors))

print("QueryAudit passed: tests succeeded and the fresh audit is PASS")
PY
```

```bash
bash ci/query-audit.sh
```

게이트를 통과하면 이렇게 출력돼요.

```text
QueryAudit passed: tests succeeded and the fresh audit is PASS
```

`--rerun-tasks --no-build-cache`는 Gradle이 예전 테스트 결과를 재사용하지 않고 테스트를 실제로 실행하게
해요. 잘못된 JSON, 판정 없음, `FAIL`, `INCONCLUSIVE`는 모두 스크립트를 실패시켜요. JUnit 명령이 성공한
것만으로는 부족해요. 테스트 본문이 성공한 뒤에도 리포트 마무리가 실패할 수 있어요. 테스트 명령의 종료
상태와 최종 JSON 판정을 함께 게이트로 쓰세요. `[OK]` 같은 테스트별 콘솔 줄은 나중의 예산 단언 실패보다
먼저 찍힐 수 있어서 최종 판정이 아니에요.

이 스크립트는 단일 모듈 Gradle 프로젝트 기준이에요. 멀티 모듈 빌드에서는 `:orders:test`처럼 그 모듈의
테스트 태스크를 쓰고, 삭제와 Python 검사 양쪽에 모듈 기준의 리포트 경로를 쓰세요. wrapper가 저장소
루트에만 있다면 스크립트를 루트에서 실행하세요.

Maven이라면 Gradle 명령을 `mvn -Dtest=QueryBudgetTest test`로 바꾸고, 프로필, 산출물 정리, Python 검사는
그대로 두세요. 설정한 리포트 디렉터리도 같아요.

## 4. CI가 실패해도 리포트 남기기 {#4-keep-the-report-when-ci-fails}

Java와 테스트 데이터베이스가 준비된 뒤, 기존 GitHub Actions 잡에 다음 단계를 추가하세요.

```yaml
- name: Check query budget
  run: bash ci/query-audit.sh

- name: Keep QueryAudit evidence
  if: always()
  uses: actions/upload-artifact@v4
  with:
    name: query-audit-report
    path: build/reports/query-audit/report.json
    if-no-files-found: error
```

스크립트에서 경로를 바꿨다면 여기서도 같은 모듈 기준 경로를 쓰세요. MySQL과 PostgreSQL의 전체 잡 예시는
[CI/CD 가이드](ci-cd.md)에 있어요.

## 게이트가 실패했을 때 {#when-the-gate-fails}

| 결과 | 다음 단계 |
|---|---|
| 쿼리 예산 초과 | 문장 목록을 읽고, 늘어난 접근을 고친 뒤 같은 테스트를 다시 실행하세요. 동작을 의도적으로 바꾼 경우에만 예산을 바꾸세요 |
| `INCONCLUSIVE` | 리포트의 `incompleteReasons`를 읽고 빠진 캡처나 입력을 되살리세요 |
| 리포트가 없거나 잘못됨 | 선택한 라이브러리 버전, 활성 Spring 프로필이나 테스트 JVM 설정, 출력 경로, 테스트 로그를 확인하세요 |
| 감사는 `PASS`인데 테스트 명령이 실패 | 일반 테스트 실패를 고치세요. 감사가 통과해도 그 실패를 덮지 않아요 |

`PASS`는 보고된 감사와 설정된 정책에 대한 판정이에요. 데이터베이스를 쓰는 모든 테스트가 감사됐다는 증명은
아니에요. 빠지거나 건너뛴 감사도 잡을 실패시켜야 한다면 [기대 테스트 목록](audit-coverage.md)을 추가하세요.

테스트가 늘어나면 [개수 계약을 기록](contracts.md)하고 그 변경을 PR에서 리뷰하세요.
[리포트 비교](reports.md#delta-verdict-compare-two-runs)를 추가하면 새 탐지 결과를 확인하면서, 빠진 감사
증거나 바뀐 비교 입력은 받아들이지 않을 수 있어요. 쿼리 수 제한은 계속 예산이나 계약이 강제하고, 비교기는
전체 쿼리 수 변화만으로 게이트하지 않아요.
