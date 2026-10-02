---
title: 고친 경로 고정하기
description: 기존 스위트에서 N+1을 찾고, 고친 경로를 예산이나 계약으로 고정하고, CI에서 게이트해요.
source_digest: be4bcc8325aa
---

# 고친 경로 고정하기 {#lock-a-fixed-path}

QueryAudit은 세 단계로 동작해요. N+1을 찾고, 고친 상태를 고정하고, PR을 게이트해요. 이 페이지는 이
세 단계를 기존 Spring Boot 스위트에 적용해요.

| 지키고 싶은 것 | 추가할 것 | 테스트가 실패하는 경우 |
|---|---|---|
| 고친 N+1이 다시 생기지 않게 | 클래스에 `@QueryAudit` | 같은 SELECT가 한 호출 위치에서 다른 값으로 3번 이상 실행될 때 |
| 조회 경로의 쿼리 수를 제한하고 쓰기를 막기 | `@ExpectQueries(select = 2, insert = 0, update = 0, delete = 0)` | SELECT가 상한을 넘거나, 금지한 쓰기가 실행될 때 |
| 요청이나 작업의 정확한 쿼리 수 | [`QueryContractScope`](contracts.md#contract-a-request-or-job)와 기록된 계약 | 개수가 늘든 줄든 바뀌었는데 다시 기록하지 않았을 때 |
| 감사하는 모든 테스트의 정확한 쿼리 수 | 스위트 전체에 [기록 모드](contracts.md#recording) | 기록된 테스트의 개수가 바뀔 때 |

## N+1 찾기 {#find-the-n1}

강제하기 전에 먼저 조사하세요. `@EnableQueryInspector`는 탐지 결과를 보고만 하고 테스트를 실패시키지
않아요. 명시한 예산과 계약은 여전히 실패해요.

```java
@SpringBootTest
@EnableQueryInspector
class OrderServiceQueryTest {
    // Existing orderService injection and test fixtures.

    @Test
    void recentOrders() {
        var orders = orderService.findRecentOrders();
        orders.forEach(order -> assertFalse(order.getItems().isEmpty()));
    }
}
```

확정된 `n-plus-one` 탐지 결과는 반복된 SELECT와 그걸 실행한 호출 스택을 알려 줘요.
연관 엔티티를 여러 개 넣고, 애플리케이션이 실제로 하는 연관관계 접근을 실행하세요. 행이 하나면 반복문이
반복될 수 없어요. 매핑이 요구하면 연관관계 접근은 트랜잭션 안에서 하고, 쓰기가 감사 구간 안에서
실행되도록 대기 중인 JPA 변경을 flush하세요.
[N+1 탐지 방식](../detections/n-plus-one.md) · [SQL과 호출 위치 읽기](reports.md)

고친 뒤에는 클래스를 `@QueryAudit`으로 바꿔서, 같은 문제가 돌아오면 테스트가 실패하게 하세요.
선택 규칙을 켰다면 `@QueryAudit(failOn = IssueType.N_PLUS_ONE)`으로 N+1에서만 실패하게 할 수 있어요.

## 조회 경로에 예산 걸기 {#budget-a-read-path}

예산은 테스트 메서드에 직접 적는 상한이에요.

```java
@Test
@ExpectQueries(select = 2, insert = 0, update = 0, delete = 0)
void recentOrdersStayWithinBudget() {
    var orders = orderService.findRecentOrders();
    assertFalse(orders.isEmpty());
    orders.forEach(order -> assertFalse(order.getItems().isEmpty()));
}
```

`io.queryaudit.junit5.EnableQueryInspector`와 `io.queryaudit.junit5.ExpectQueries`를 쓰고, 단언은
`org.junit.jupiter.api.Assertions`에서 가져와요. 조회 경로가 UPDATE를 실행하기 시작하면 진단 메시지에
그 문장이 들어가요. 예시 일부:

```text
QueryAudit: recentOrdersStayWithinBudget() exceeded its query budget.
UPDATE: executed 1, expected at most 0.
  update orders set last_viewed_at = ? where id = ?
```

이 값들은 상한이에요. `select = 2`는 SELECT 0개, 1개, 2개를 모두 허용하고, 정확히 2개를 요구하지는
않아요. `0`은 그 종류의 문장을 금지하고, 적지 않은 속성은 제한하지 않아요. `total`은 모든 문장을 합쳐서
제한해요. 예산은 캡처된 문장 수를 세고, 반환된 행 수나 걸린 시간은 세지 않아요. 0.7.2부터는
`exact = true`로 적은 개수를 정확히 요구할 수 있어서, 쿼리 하나가 사라진 경우도 실패해요.

## 요청이나 작업을 계약으로 고정하기 {#contract-a-request-or-job}

계약은 QueryAudit이 대신 기록해 주는 개수예요. 예산과 비교하면:

| | 예산 | 계약 |
|---|---|---|
| 숫자를 정하는 쪽 | 내가 어노테이션에 적어요 | 기록 모드가 커밋할 파일에 적어요 |
| 실패하는 경우 | 상한을 넘을 때. `exact = true`면 개수가 바뀔 때 | 늘든 줄든 개수가 바뀔 때 |
| 범위 | 테스트 메서드 하나 | 테스트 메서드, 요청, 작업, 여정 |
| 잘 맞는 경우 | "이 경로에서는 쓰기 금지" 같은 몇 가지 규칙 | 요청 수백 개를 추적하면서 변경을 하나하나 리뷰할 때 |

테스트 메서드에는 보통 픽스처 준비, 테스트할 요청, 단언이 섞여 있어요.
`QueryContractScope`는 넘겨준 작업만 세요.

```java
@Autowired QueryContractScope contracts;

@Test
void listsLinks() throws Exception {
    contracts.verify("link-list", () -> mockMvc.perform(get("/api/v1/links")))
        .andExpect(status().isOk());
}
```

`-DqueryAudit.contracts.record=true`(Maven) 또는 `-PqueryAudit.contracts.record=true`
([속성 브리지](ci-cd.md#plain-junit-build-tool-setup)를 설정한 Gradle)로 기록하고, 파일을 커밋한 뒤 이후
변경은 diff로 리뷰하세요. 기록된 계약이 없는 구간은 추가할 줄을 알려 주면서 실패해요.
[계약](contracts.md)

## CI로 옮기기 {#move-to-ci}

작업 하나는 [첫 CI 체크](first-ci-check.md)로 시작하세요. 특정 테스트들이 실제로 감사됐는지 잡이
증명해야 한다면 [기대 테스트 커버리지](audit-coverage.md)로 넓히세요. 리뷰어가 실행 사이의 새 탐지 결과,
해결된 탐지 결과, 남아 있는 탐지 결과를 구분해야 한다면 [리포트 비교](reports.md#delta-verdict-compare-two-runs)를
추가하세요.

탐지 결과 베이스라인과 계약은 목적이 달라요. [CI 도입 표](ci-cd.md#gradual-adoption)를 보면 새 쿼리 수를
모르는 사이에 받아들이지 않으면서 둘 중 하나를 고를 수 있어요.

## 선택 규칙의 탐지 결과 살펴보기 {#investigate-optional-findings}

기본 프로필은 N+1 규칙만 실행해요. [선택 규칙](../detections/overview.md)을 켰다면, 강제하기 전에 각
탐지 결과를 실제 증거와 비교해 보세요.

| 조사할 것 | 시작점 | 확인할 것 |
|---|---|---|
| 인덱스가 없을 수 있다는 결과 | 테스트 데이터베이스에 맞는 모듈을 설치해요. [누락된 인덱스](../detections/missing-index.md) | 기존 복합 인덱스, 대표 쿼리, 실행 계획 |
| 도입할 때 이미 있는 탐지 결과 | 특정 탐지 결과를 검토하고 승인해요. [탐지 결과 억제](suppressing.md) | 탐지 결과 베이스라인은 예산이나 계약을 바꾸지 않아요 |

인덱스 탐지 결과는 애플리케이션과 같은 데이터베이스 계열과 관련 스키마로 확인하고, 인덱스를 추가하기 전에
기존 복합 인덱스와 쿼리 계획을 살펴보세요. 탐지 결과는 접근 경로를 조사할 이유이지, 제안된 스키마 변경이
운영 지연 시간을 줄인다는 증거는 아니에요.
