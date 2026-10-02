---
title: QueryAudit — 머지 전에 N+1 쿼리를 잡아요
description: JUnit 5 테스트에서 N+1 쿼리를 호출 위치로 찾고, 고친 쿼리 수를 계약으로 고정하고, CI에서 PR을 게이트해요.
source_digest: 6db3750c5fd1
hide:
  - navigation
  - toc
---

<div class="qa-hero">
  <div class="qa-hero__copy">
    <p class="qa-eyebrow">JUNIT 5 테스트의 N+1과 쿼리 회귀</p>
    <h1>머지 전에<br>N+1을 잡아요.</h1>
    <p class="qa-hero__lead">
      QueryAudit은 N+1을 일으킨 줄을 알려 주고, 고친 쿼리 수를 모든 PR이 지켜야 하는
      계약으로 고정해요.
    </p>
    <div class="qa-hero__actions">
      <a href="getting-started/installation/" class="md-button md-button--primary">내 테스트에 추가 →</a>
      <a href="getting-started/quickstart/" class="md-button">예제 실행</a>
    </div>
    <p class="qa-hero__note">Java 17+ · JUnit 5 · 테스트 의존성만</p>
  </div>
  <div class="qa-terminal" aria-label="N+1이 호출 위치에서 테스트를 실패시키고, 고친 테스트는 통과해요">
    <div class="qa-terminal__bar">
      <span>반복문이 주문마다 고객을 하나씩 불러와요</span>
      <span class="qa-terminal__dots" aria-hidden="true">● ● ●</span>
    </div>
    <pre><code>@Test @QueryAudit
void listsOrderSummaries() { … }

<span class="qa-terminal__error">[ERROR] N+1 Query detected (table: customers)
  The same SELECT ran 5 times from one call site
  at OrderService.recentOrderSummaries:42</span>

<span class="qa-terminal__muted">고객을 한 번에 불러오고 테스트를 다시 실행해요.</span>

<span class="qa-terminal__success">"outcome": "PASS"</span></code></pre>
    <div class="qa-terminal__footer">호출 위치의 N+1 → 통과한 JSON 판정</div>
  </div>
</div>

<div class="qa-workflow-nav" markdown>

[**01** N+1 찾기](#find-an-n1)
[**02** 고친 상태 고정하기](#lock-the-fix)
[**03** PR 게이트하기](#gate-the-pull-request)
[**04** 실제 서비스에서 쓰는 모습](#used-on-a-production-service)

</div>

## N+1 찾기 {#find-an-n1}

[스타터를 설치](getting-started/installation.md)한 뒤, 연관 데이터를 읽는 테스트에 `@QueryAudit`을
붙여요.

```java
@SpringBootTest
@QueryAudit
class OrderServiceTest {
    @Autowired OrderService orderService;

    @Test
    void listsOrderSummaries() {
        assertEquals(5, orderService.recentOrderSummaries().size());
    }
}
```

설정이 없으면 규칙 하나만 실행돼요. 같은 SELECT가 서로 다른 값으로, 애플리케이션 호출 스택 전체가 같은
한 위치에서 3번 이상 실행되면 N+1이에요. `recentOrderSummaries()`가 반복문 안에서 주문마다 고객을
불러오면, 테스트는 그 줄을 가리키며 실패해요.

```text
QueryAudit detected 1 issue(s) in listsOrderSummaries():

  [ERROR] N+1 Query detected (table: customers)
    Detail: The same SELECT ran 5 times from one call site
    Suggestion: Load the rows once before the loop: JOIN FETCH, @EntityGraph, or one query with an IN list.
    Call stack:
      at com.example.order.OrderService.recentOrderSummaries:42
      at com.example.order.OrderServiceTest.listsOrderSummaries:18
```

`IN (?, ?, ...)`로 묶어서 가져오는 건 문제가 아니라 해결책이라서, `@BatchSize`와 배치 페치는 잡지 않아요.
`OFFSET`으로 페이지를 넘기는 것도 N+1이 아니고, 같은 값으로 같은 조회를 반복하는 건 테스트를 실패시키지
않는 INFO로만 보고해요. Hibernate 지연 로딩 이벤트는 어떤 연관관계를 미리 가져와야 하는지 알려 주는 INFO
줄을 더해요. 기존 스위트를 실패시키지 않고 조사만 하려면 `@QueryAudit` 대신 `@EnableQueryInspector`를
쓰세요.

[N+1 탐지 방식 →](detections/n-plus-one.md)
· [SQL과 호출 위치 읽기](guide/reports.md)

## 고친 상태 고정하기 {#lock-the-fix}

**예산**은 테스트에 직접 적는 상한이에요. SELECT는 최대 2개, 쓰기는 없음:

```java
@Test
@ExpectQueries(select = 2, insert = 0, update = 0, delete = 0)
void listsOrderSummaries() {
    assertEquals(5, orderService.recentOrderSummaries().size());
}
```

반복문이 다시 생기면, 돌려주는 데이터가 여전히 맞더라도 테스트가 실패해요.

```text
SELECT: executed 6, expected at most 2.
```

**계약**은 QueryAudit이 대신 기록해 주는 개수예요. `QueryContractScope`는 픽스처 준비나 단언은 빼고,
넘겨준 요청이나 작업만 세요. 이름을 적은 스레드 풀의 작업도 기다렸다가 세요.

```yaml
query-audit:
  await-executors: [taskExecutor]
  contracts:
    path: src/test/resources/query-contracts
```

```java
@Autowired QueryContractScope contracts;

@Test
void listsLinks() throws Exception {
    contracts.verify("link-list", () -> mockMvc.perform(get("/api/v1/links")))
        .andExpect(status().isOk());
}
```

`-DqueryAudit.contracts.record=true`로 한 번 기록하고 파일을 커밋하면, 이후 변경은 PR에서 한 줄로
리뷰할 수 있어요.

```diff
-@junit | link-list | 2 | 0 | 0 | 0 | 2
+@junit | link-list | 3 | 0 | 0 | 0 | 3
```

다시 기록하지 않으면 테스트가 차이와 늘어난 SQL을 보여 주며 실패해요. 테스트 메서드 전체의 계약도 같은
파일에 둘 수 있어요.

공개된 라이브러리와 인메모리 H2로 **예산 실패를 직접 확인**해 보세요.

```sh
git clone https://github.com/haroya01/query-audit.git
cd query-audit
./gradlew -p examples/first-audit test -PextraWrite=true --rerun-tasks
```

```text
UPDATE: executed 1, expected at most 0.
```

`-PextraWrite=true` 없이 다시 실행하면 테스트가 통과해요.

[고친 경로 고정하기 →](guide/choose-your-workflow.md)
· [계약](guide/contracts.md)
· [빠른 시작](getting-started/quickstart.md)

## PR 게이트하기 {#gate-the-pull-request}

기준 브랜치와 PR에서 각각 JSON 리포트를 저장하고, 같은 버전의 core JAR로 비교해요.

```sh
java -cp "$QUERY_AUDIT_CORE_JAR" \
  io.queryaudit.core.reporter.ReportComparator before.json after.json verdict.json
```

| PR이… | 결과 |
| --- | --- |
| 확정된 탐지 결과를 추가하지 않고, 모든 예산과 계약을 지킴 | `PASS` |
| 예산이나 계약을 깨뜨림 | `FAIL` |
| [기대 테스트 목록](guide/audit-coverage.md)에 있는 테스트를 건너뛰거나 잃음 | `INCONCLUSIVE` |
| 규칙, 기준값, 필수 분석 입력을 바꿈 | `INCONCLUSIVE` |

특정 탐지 결과가 사라졌는지 증명하려면 `--require-resolved <findingId>`를 추가하세요.

[첫 CI 체크 설정하기 →](guide/first-ci-check.md)
· [두 실행 비교하기](guide/ci-cd.md)
· [비교 입력](guide/comparison-inputs.md)

## 실제 서비스에서 쓰는 모습 {#used-on-a-production-service}

QueryAudit은 Spring Boot와 MySQL로 만든 실제 운영 URL 단축 서비스
[short-link](https://github.com/haroya01/short-link)에서 직접 쓰고 있어요. 그 테스트 스위트가 0.7의 수용
테스트예요.

- 같은 감사 테스트 45개에서, 기본 설정의 확정 탐지 결과가 0.6.0의 142건에서 N+1 1건으로 줄었어요. 일괄
  링크 생성이 새 코드마다 `findByShortCode`로 조회하는 문제예요. 같은 반복문이 한 사용자에 대해
  `countByUserId`도 반복하는데, 이건 INFO로 보고돼요. 0.6.0은 둘 다 보고하지 못했어요.
- 직접 짠 헬퍼에서 `QueryContractScope`로 옮긴 뒤에도 HTTP 쿼리 계약 584개의 개수가 모두 그대로였어요.
  `QueryContractScope`는 QueryAudit의 내부 클래스가 필요 없어요.
- 링크 생성에 SELECT 하나를 일부러 추가하자 테스트 클래스 8개에서 계약 13개가 실패했고, 각 실패가 반복된
  문장과 그 호출 위치를 보여 줬어요.

---

**N+1 말고도 필요하다면?** 인덱스, `EXPLAIN`, SQL 스타일 규칙은 `profile: strict`나 `enabled-rules`로
켜요. [선택 규칙](detections/overview.md)

**설정 확인하기:** [Spring Boot](getting-started/spring-boot.md)
· [순수 JUnit](getting-started/installation.md#plain-junit-5)
· [QuickPerf와 비교](guide/coming-from-quickperf.md)
· [버전](getting-started/versions.md)
· [알려진 한계](guide/limitations.md)
