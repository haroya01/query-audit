---
title: 쿼리 계약
description: 테스트, 요청, 작업의 쿼리 수를 계약 파일에 기록하고, 바뀌면 실패시키고, 의도한 변경은 diff로 리뷰해요.
source_digest: d043f05b3afb
---

# 쿼리 스냅샷 계약 {#query-snapshot-contracts}

감사하는 테스트의 SELECT/INSERT/UPDATE/DELETE 개수를 `.query-audit-contracts`에 기록해요.
나중에 개수가 늘거나 줄면 테스트가 실패해요. 의도한 변경은 코드 변경과 함께 리뷰하세요.

```diff
-@junit | [engine:junit-jupiter]/[class:com.example.OrderServiceTest]/[method:placeOrder()] | 2 | 1 | 1 | 0 | 4
+@junit | [engine:junit-jupiter]/[class:com.example.OrderServiceTest]/[method:placeOrder()] | 2 | 3 | 1 | 0 | 6
```

이 예시는 `placeOrder()`에 INSERT가 2개 늘어난 경우예요. 계약은 **개수**를 비교해요. SQL 문장, WHERE
조건, 반환된 행, 그 밖의 모든 데이터베이스 동작을 비교하지는 않아요. 일반적인 결과 단언은 그대로 두고,
쿼리가 줄어도 스냅샷을 고치지 않고 통과해야 한다면 [인라인 예산](annotations.md#expectqueries)을 쓰세요.

!!! note "버전 범위"
    쿼리 스냅샷 계약은 0.5에서 도입됐어요. 0.6부터는 안정적인 JUnit ID로 기록하고, 정책 파일 구분자가
    들어간 ID를 이스케이프하고, 쿼리를 하나도 실행하지 않은 감사 테스트도 포함해요. 0.5는 클래스와 표시
    이름으로 식별하고 쿼리가 0개인 테스트는 건너뛰어요.

## 기록하기 {#recording}

스위트를 기록 모드로 한 번 실행해요. Gradle 명령은 CI 가이드의
[`Test.systemProperty` 브리지](ci-cd.md#plain-junit-build-tool-setup)를 설정했다고 가정해요.

=== "Gradle"

    ```bash
    ./gradlew test -PqueryAudit.contracts.record=true
    ```

=== "Maven"

    ```bash
    mvn test -DqueryAudit.contracts.record=true
    ```

완료된 감사 테스트마다 SELECT/INSERT/UPDATE/DELETE 개수가 작업 디렉터리의 `.query-audit-contracts`에
기록돼요. 파이프로 구분하고 정렬돼 있어서 사람이 리뷰하기 쉬워요.

```
# QueryAudit Query Contracts
# Format: identityType | identityValue | selectCount | insertCount | updateCount | deleteCount | totalCount
# @junit identityValue escapes: \| (pipe), \\ (backslash), \r (CR), \n (LF)
@junit | [engine:junit-jupiter]/[class:com.example.OrderServiceTest]/[method:findOrders()] | 3 | 0 | 0 | 0 | 3
@junit | [engine:junit-jupiter]/[class:com.example.OrderServiceTest]/[method:placeOrder()] | 2 | 1 | 1 | 0 | 4
```

안정적인 JUnit ID에 정책 파일 구분자나 줄바꿈이 들어 있어도 사람이 읽을 수 있는 한 줄로 남아요.
`@junit` 식별 값 안에서는 파이프를 `\|`, 백슬래시를 `\\`, 캐리지 리턴을 `\r`, 줄바꿈을 `\n`으로 적어요.
이 이스케이프는 `@junit` 식별 값에만 적용되고, 다섯 개의 개수 필드와 예전 형식의 줄은 이스케이프하지
않아요. 알 수 없거나 불완전한 이스케이프가 있으면 정책 파일이 유효하지 않게 되고, 진단 메시지가 문제의
줄을 알려 줘요.

파일을 커밋하세요. [`mode: all`](configuration.md#audit-coverage-mode)과 함께 쓰면 감사하는 스위트
전체의 개수를 기록할 수 있어요. 기대하는 테스트를 반드시 실행하게 하려면
[기대 테스트 목록](audit-coverage.md)을 쓰세요. 계약 항목만으로는 그 테스트가 건너뛰어져도 실패하지 않아요.

## 강제하기 {#enforcement}

이후 실행마다 기록된 항목이 있는 테스트를 그 계약과 비교해요. 개수가 다르면 차이와 함께 실패해요.
늘어난 경우에는 그 종류의 캡처된 문장과, 있으면 첫 호출 위치도 함께 보여 줘요. 진단 예시(SQL 목록 일부):

```
QueryAudit: placeOrder() deviates from its recorded query contract (.query-audit-contracts).
  INSERT: contract 1, executed 3 (+2)
    insert into order_items (order_id, sku) values (?, ?)
      at com.example.OrderService.placeOrder:87
If the change is intended, re-record the contracts with -DqueryAudit.contracts.record=true and review the file diff.
```

호출 위치가 JDBC 프록시를 가리킬 수 있어요. 애플리케이션 호출 위치는
[JSON 리포트](reports.md#read-a-policy-failure)의 `reports[].queries[].stackTrace`에서 확인하세요.

마지막 줄은 테스트 JVM 속성 이름을 알려 줘요. 브리지를 설정한 Gradle 프로젝트는
`-PqueryAudit.contracts.record=true`로 다시 실행하고, Maven 프로젝트는 진단에 나온 `-D` 형식을 쓰세요.

`@ExpectQueries`, 폐기 예정인 `@ExpectMaxQueryCount`, 스냅샷 계약의 실패는 탐지 결과가 아니라 테스트
단언이에요. 규칙 프로필, `disabled-rules`, `suppress-patterns`, 심각도 덮어쓰기, 이슈 베이스라인은 이
결과를 바꾸지 않아요. 개수 변경이 의도한 것이라면 선언한 예산을 고치거나 계약을 다시 기록하세요.

강제 규칙:

- **양방향 모두 실패해요.** 계약보다 쿼리가 적어도 실패해요. 스냅샷이라서 줄어든 것도 계약 diff에 남겨야
  해요.
- **항목이 없는 테스트는 강제하지 않아요.** 새 테스트가 소급해서 실패하지 않고, 다시 기록하면 포함돼요.
- **`@ExpectQueries`가 우선해요.** 인라인 예산이 있는 메서드는 파일 계약에서 빠져요. 어노테이션이 더
  구체적인 선언이기 때문이에요.
- **기록 모드는 개수 비교를 건너뛰어요.** 그래서 계약과 어긋난 스위트도 다시 기록할 수 있어요. 다만 실행되지
  않은 테스트의 항목을 유지하기 때문에 기존 파일은 읽을 수 있고 유효해야 해요.
- **유효하지 않은 파일은 실행을 실패시켜요.** 기존 계약 파일이 잘못됐거나 읽을 수 없으면 감사 초기화가
  멈춰요. 오류는 파일을 알려 주고, 잘못된 항목이면 줄 번호도 알려 줘요. 파일이 없는 건 유효하고, 아직
  기록한 계약이 없다는 뜻이에요.
- **기록 실패는 실행을 실패시켜요.** 요청한 계약 파일이나 개수 베이스라인 파일을 저장하지 못하면 런처가
  실패를 보고하고, 스위트 판정은 `POLICY_WRITE_FAILED`와 함께 `INCONCLUSIVE`가 돼요. 보고 전용 모드도 이
  실패를 숨기지 않아요. 정책 파일을 커밋하기 전에 저장 위치를 고치고 다시 기록하세요.

## 업데이트하기 {#updating}

다시 기록하고 diff를 리뷰해요.

=== "Gradle"

    ```bash
    ./gradlew test -PqueryAudit.contracts.record=true
    git diff .query-audit-contracts
    ```

=== "Maven"

    ```bash
    mvn test -DqueryAudit.contracts.record=true
    git diff .query-audit-contracts
    ```

기록은 병합 방식이에요. 실행된 테스트는 갱신되고, 실행되지 않은 테스트의 항목은 그대로 남아요.

### 0.5에서 기록한 파일 옮기기 {#migrating-files-recorded-by-05}

0.6은 0.5의 `testClass | displayName | ...` 형식 줄도 계속 읽어요. 테스트에 안정적인 ID 줄이 없으면
정확히 일치하는 예전 줄로 강제할 수 있어요. 다만 그 식별 방식은 패키지나 같은 표시 이름을 구분하지 못해서
QueryAudit이 이전 경고를 출력해요. 0.6으로 기록하면 `@junit | <uniqueId>` 줄이 추가되고, 이후 실행은 그
줄을 먼저 써요. 일부만 기록해도 실행되지 않은 테스트를 잃지 않도록 예전 줄은 파일에 남아요. 안정적인 줄을
기록한 뒤에는 0.5와 0.6 실행기를 섞어 쓰지 마세요. 0.5 리더는 이스케이프되지 않은 `@junit` 줄을 일곱 개의
일반 필드로 읽을 수는 있지만, `@junit`을 클래스 이름으로 취급해서 테스트와 연결하지 못해요. 파이프나
줄바꿈에 대한 0.6의 이스케이프도 해석하지 못해요. 안정적인 줄에 의존하기 전에 모든 실행기를 올리세요.
남아 있는 예전 줄의 백슬래시는 원래의 문자 그대로 의미를 유지하고, 안정적인 ID 이스케이프가 소급
적용되지는 않아요.

예전 줄 하나가 아직 필요한 테스트가 있는데 그 줄이 다른 안정적인 JUnit ID와도 일치하면, 모호하다는
진단과 함께 실행이 실패해요. 일치하는 모든 테스트에 자기 안정적인 줄이 생기면 남은 예전 줄은 무시돼요.
예전 계약 하나가 테스트 두 개에 적용되게 두지 말고, 감사하는 스위트 전체를 기록 모드로 실행해서 새로 생긴
안정적인 줄을 리뷰하세요. 0.6으로 처음 기록하기 전에 표시 이름을 바꿨다면 예전 줄과 안전하게 연결할 수
없으니, 업그레이드할 때 스위트 전체를 한 번 다시 기록하세요.

## 설정 {#configuration}

| 설정 | 설명 |
|---|---|
| `query-audit.contracts.path` / `queryAudit.contracts.path` | 계약 파일 하나, 또는 폴더 안의 `*.contracts` 파일과 `.query-audit-contracts`를 모두 읽는 폴더. 기본값은 작업 디렉터리의 `.query-audit-contracts` |
| `queryAudit.contracts.record` | 명령줄 플래그. `true`면 강제하는 대신 계약을 기록하거나 갱신해요 |

테스트 메서드와 [구간 계약](#contract-a-request-or-job)은 같은 저장소와 형식을 함께 써요.
기록하면 이미 그 항목이 있는 파일의 항목을 갱신하고, 새 항목은 설정한 파일이나 설정한 폴더 안의
`.query-audit-contracts`에 써요. 실패 메시지는 그 계약이 들어 있는 파일을 알려 줘요. 설정 이름은
[한 가지 규칙](configuration.md#setting-names)을 따라요.

## 요청이나 작업을 계약으로 고정하기 {#contract-a-request-or-job}

테스트 메서드에는 보통 픽스처 준비, 테스트할 요청, 데이터베이스 단언이 섞여 있어요.
`QueryContractScope`(0.7.0)는 넘겨준 작업만 세요. Spring Boot 스타터가 설정한 계약을 쓰는 빈으로
제공해요.

```yaml
query-audit:
  await-executors: [taskExecutor]
  contracts:
    path: src/test/resources/query-contracts
```

```java
@SpringBootTest
@EnableQueryInspector
class LinkApiTest {
    @Autowired QueryContractScope contracts;

    @Test
    void listsLinks() throws Exception {
        contracts.verify("link-list", () -> mockMvc.perform(get("/api/v1/links")))
            .andExpect(status().isOk());
    }

    @Test
    void signsUp() throws Exception {
        try (var journey = contracts.open("signup-journey")) {
            mockMvc.perform(post("/api/v1/signup").content(body)).andExpect(status().isCreated());
            mockMvc.perform(get("/api/v1/me")).andExpect(status().isOk());
        }
    }
}
```

`verify`는 작업의 결과를 돌려줘요. `open`은 여러 요청을 묶고 블록이 닫힐 때 검증해요. 블록 안에서 실패가
나면 그 실패가 주 예외로 남아요. 테스트가 먼저 `queries()`나 `counts()`를 봐야 한다면 `capture`와
`verify(captured)`로 두 단계를 나눌 수 있어요. Spring 없이 쓸 때는 DataSource에 연결한 인터셉터로
`QueryContractScope.of(interceptor, path)`를 만들어요.

`await-executors`에는 요청이 작업을 넘기는 스레드 풀 이름을 적어요. 구간은 그 풀들이 한가해질 때까지
기다렸다가 세기를 멈추고, 30초가 지나면 바쁜 풀의 이름과 함께 실패해요. 직접 기다리는 방법을 쓰려면
`awaitingCompletion(Runnable)`을 쓰세요. 같은 설정은 감사하는 테스트 메서드도 그 풀들을 기다리고 그
SQL을 세게 만들어요. [백그라운드 작업](configuration.md#background-work)을 참고하세요.

구간 ID도 테스트 메서드와 같은 줄 형식을 써요.

```text
@junit | link-list | 2 | 0 | 0 | 0 | 2
```

| 상황 | 결과 |
|---|---|
| 개수가 일치 | 통과하고 작업의 결과를 돌려줘요 |
| 개수가 늘거나 줄었음 | 차이, 늘어난 종류의 SQL, 계약 파일을 담은 `QueryContractViolation` |
| 구간 ID에 대한 항목이 없음 | 추가할 측정값 줄을 담은 `QueryContractViolation` |
| 같은 ID가 두 파일에 있음 | `IllegalStateException` |
| 캡처가 `max-queries`에 도달함 | `QueryContractViolation`. 잘린 캡처로는 계약을 검증할 수 없어요 |
| 구간이 아직 열려 있음 | 열린 구간의 이름을 담은 `IllegalStateException` |

`QueryContractViolation`은 `AssertionError`예요. 0.7.2부터는 이 예외로 끝난 테스트가 있으면 감사
어노테이션이 없는 테스트 클래스라도 실행의 JSON 판정이 `FAIL`이 되고, 콘솔 요약에 그 테스트가 나와요.
`assertThrows`처럼 테스트가 직접 위반을 잡아서 확인하는 경우는 해당하지 않아요. 0.7.1에서는 실패한
테스트로만 드러났어요.

기록된 계약이 없는 테스트 메서드는 통과해요. 계약이 감사하는 모든 테스트에 자동으로 적용되기 때문이에요.
계약이 없는 구간은 실패해요. 테스트가 계약을 요청했기 때문이에요.

구간은 열려 있는 동안 감사 대상 DataSource를 쓰는 모든 스레드의 SQL을 세고, 그 SQL 때문에 바깥의
`@QueryAudit`이나 `@EnableQueryInspector` 감사가 불완전해지지 않아요. 구간을 쓰는 테스트는 격리된
데이터베이스에서 순차로 실행하고, 스케줄러는 꺼 두세요.

## 계약과 관련 기능 비교 {#contracts-vs-related-features}

| | 범위 | 실패하는 경우 | 업데이트 방법 |
|---|---|---|---|
| **계약** | 기록된 모든 테스트 | 개수가 어느 방향으로든 바뀔 때 | 다시 기록하고 파일 diff 리뷰 |
| [`@ExpectQueries`](annotations.md#expectqueries) | 메서드 하나 | 예산을 넘을 때. `exact = true`면 개수가 다를 때 | 어노테이션 수정 |
| [`QueryContractScope`](#contract-a-request-or-job) | 요청, 작업, 여정 하나 | 개수가 바뀌거나 계약이 없을 때 | 다시 기록하고 파일 diff 리뷰 |
| 개수 베이스라인(`queryAudit.counts.record`), 0.7.0부터 폐기 예정 | 베이스라인이 기록된 테스트 | 기준값을 넘는 회귀 탐지 결과. 탐지 결과 정책을 따라요 | 베이스라인 갱신. 계약으로 옮기세요 |
| [이슈 베이스라인](suppressing.md) | 탐지 결과 | 새 탐지 결과 | 승인 |
