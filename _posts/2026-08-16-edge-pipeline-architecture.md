---
title: "EDGE 데이터 파이프라인 아키텍처"
date: 2026-08-16 20:00:00 +0900
categories: [dev-log, data-pipeline]
tags: [edge, data-pipeline, architecture, aws]
image:
  path: /assets/img/posts/edge-pipeline-architecture/thumbnail.webp
---

지금 이 파이프라인은 장이 열려 있는 동안 1분마다 돈다. 장전에 그날의 세션을 계획하는 task가 있고, 장중 내내 살아 있는 worker가 있고, 무엇을 계획했고 무엇이 실제로 끝났는지를 적는 원장이 있다.

이 구조를 처음부터 그려놓고 시작한 게 아니다. 시작은 한 종목을 하루 한 번 분석하는 일 배치였고, 단계 네 개짜리 워크플로 하나면 충분했다.

네 번 다시 그렸다. 매번 기술을 늘리고 싶어서가 아니라, 팀이 요구하는 시간 단위가 바뀌었기 때문이다.

## 하루 한 번에서 장중 1분까지, 네 번 다시 그렸다 {#architecture-evolution}

| 버전 | 팀이 요구한 것 | 기존 구조가 못 버틴 지점 | 재설계 기준 |
|---|---|---|---|
| v1 | 한 종목을 하루 한 번 | 없음 — 순서만 맞으면 됐다 | 단일 워크플로에 4단계 |
| v2 | 여러 ETF, 이어서 장중 1분 단위 설명 | 벤더 순차 호출, 수집 성공과 분석 완료가 한 실행 단위 | 최장 branch 병렬화 + 큐 경계 |
| v3 | 60초 window 안에서 처리 | 배치 원장에 분 단위 상태를 못 적음 | 상주 worker + 1분 원장 |
| v4 | 실시간·배치·운영의 공존 | 두 레인이 같은 소스를 동시에 소유 | 작업마다 단일 소유자 |

## EDGE 데이터 파이프라인 아키텍처 v1

![EventBridge가 단일 Step Functions를 트리거하고 Ingest, Transform, Feature 생성, 분석 네 단계가 좌에서 우로 순서대로 이어지는 도면. 각 단계는 ECS Fargate task이고 아래쪽 저장 계층의 S3 raw/, S3 canonical/, S3 feature, EDGE Cloud RDS로 화살표가 내려간다](/assets/img/posts/edge-pipeline-architecture/pipeline-v1-daily-batch.png)
_하루 한 번 한 종목이면 단계 넷을 한 줄로 거는 워크플로 하나가 부족할 데가 없다_

v1은 순서를 지키는 데 중점을 뒀다. EventBridge가 단일 Step Functions를 깨우면 Ingest → Transform → Feature → 분석이 차례로 돌았고, 각 단계는 ECS Fargate task 하나였다.

저장소는 `raw/`에 벤더 응답을 그대로 덧붙이고 `canonical/`은 같은 키가 다시 들어오면 멱등하게 병합하도록 갈랐다. 이 경계는 [S3와 RDB의 역할을 가르는 기준](/posts/rdb-vs-s3-data-pipeline/)에서 출발해 [raw·canonical·feature 계층](/posts/medallion-architecture/)으로 굳었다.

## EDGE 데이터 파이프라인 아키텍처 v2

![뉴스 레인과 가격 레인이 위아래로 나란히 흐르는 도면. 각 레인의 수집·조회 ECS Fargate 뒤에 AWS SQS가 하나씩 놓여 있고, 그 뒤의 consumer가 DeepSeek API를 호출하거나 분석 결과 DB에 적재한다](/assets/img/posts/edge-pipeline-architecture/pipeline-v2-queue-boundary.png)
_SQS 두 개가 들어간 자리가 수집 성공과 분석 완료를 서로 다른 실행 단위로 갈라놓은 경계다_

v2는 결합을 끊는 데 중점을 뒀다. 대상이 늘자 한 단계가 벤더를 순서대로 부르는 구조가 먼저 무너졌는데, 병렬 실행의 전체 시간은 branch의 합이 아니라 **최댓값**이라 KRX 쪽만 병렬화하면 약 580초, KIS 쪽만 병렬화하면 약 579초로 사실상 제자리였다.

수집 성공과 분석 완료가 같은 실행 단위에 묶여 있으면 분석 한 건이 느려질 때 다음 분 수집이 통째로 밀린다. 그래서 벤더별로 task를 쪼개고, 그 사이에 SQS 두 개를 넣어 뉴스는 LLM 추출 consumer에게, 가격 변동 트리거는 분석 실행 consumer에게 넘겼다.

갈라놓고 나니 커밋하는 일과 발행하는 일이 같은 트랜잭션 안에 있지 않다는 문제가 남았고, 이 틈을 [outbox로 닫는 과정](https://jingi723.github.io/posts/edge-outbox/)이 다음 결정이었다.

## EDGE 데이터 파이프라인 아키텍처 v3

![위쪽 장전 구획에서 EventBridge Scheduler가 08:40에 SFN을 깨워 Session Planner Task를 띄우고 desiredCount=1로 running/healthy를 확인한다. 아래쪽 장중 구획에서는 상주 Price Worker와 News Worker가 S3 raw/canonical에 저장하고 원장 DB에 job과 outbox를 남기며, Outbox Relay Task가 Price/News Job SQS와 DLQ로 발행해 가격 변동 판정 워커와 뉴스 추출 워커가 경쟁 소비한다](/assets/img/posts/edge-pipeline-architecture/pipeline-v3-minute-session.png)
_매 분 새로 띄우는 대신 장전에 한 번 계획하고 상주 worker가 window를 받아간다 — 도면의 가격 소스 토스증권 표기는 2026-08-01 실측 당시의 사실이다_

v3은 60초를 지키는 데 중점을 뒀다. 매 주기 컨테이너를 새로 띄우면 기동과 초기화에 쓰는 시간이 60초 window 안에서 그대로 차감된다.

그래서 계획과 실행을 시간축에서 갈랐다. 장전 08:40에 세션 플래너가 그날의 window를 한 번 계획하고, 장중에는 상주 Price·News Worker가 명시된 window를 받아서 처리한다.

worker가 상주하자 이 분의 작업을 누가 잡았고 몇 번 재시도했고 발행까지 갔는지를 적을 곳이 필요해졌는데, 일 배치 원장에는 그 칸이 없어서 session·window·job·outbox를 포함한 6테이블로 1분 원장을 새로 깔았다.

60초 상한을 정한 건 내가 아니라 벤더 유량이었다. 1분 경로의 첫 벤더는 토스증권이었는데 로컬에서 348건을 replay하니 **70.631초**로 window를 10.631초 넘겼고, 초당 14.8건이 나오는 KIS로 기본 분봉 소스를 바꿨다. 토스가 느렸던 게 아니라 60초 window라는 내 요구가 그 시점의 유량 상한 위에 있었다.

## EDGE 데이터 파이프라인 아키텍처 v4

![세션 오케스트레이션, 장중 1분 레인, 세션 비결속 분석 레인, 배치 오케스트레이션과 배치(KST) 네 영역을 한 장에 그린 도면. 장중 1분 레인의 News·Price·업종지수·iNAV·공시 Worker와 배치 레인의 수집·정제·적재 워커가 각각 S3 raw/canonical과 DB로 흐르고, 오른쪽 위 주석에 DB 아이콘은 논리 저장소이며 물리 DB 분리를 의미하지 않는다고 적혀 있다](/assets/img/posts/edge-pipeline-architecture/pipeline-v4-four-lanes.png)
_네 레인이 같은 소스와 같은 S3, 같은 원장을 나눠 쥐고 있어서 남은 문제가 속도가 아니라 소유권이 된다_

v4는 소유권을 정하는 데 중점을 뒀다. 레인을 늘리는 일 자체는 쉬웠고, 어려운 건 공시 수집처럼 같은 소스와 같은 CLI를 두 레인이 동시에 쥐는 상황이었다. 그러면 원장은 한쪽이 끝낸 일을 못 보고 영구 MISSED로 남기거나 양쪽이 각자 호출해 DART 한도를 중복으로 태우기 때문에, 작업마다 단일 소유자를 원장에 못박았다.

오케스트레이터가 성공으로 끝났다는 건 상태 머신이 끝까지 갔다는 뜻이지 데이터가 다 들어왔다는 뜻이 아니라, 계획을 먼저 적어두고 끝난 뒤에 실적과 대조하는 원장을 따로 뒀다. 2026년 8월 14일 거래일에 가격 분석 큐로 나간 job은 390건으로, 09:01부터 15:30까지의 장중 1분 window 수와 정확히 일치한다.

이 도면은 구현한 경로와 확장 목표를 함께 정리한 것이다. 네 영역이 전부 같은 완성도로 돌고 있다는 뜻은 아니다.

## 네 번 다시 그린 이유는 기술이 아니라 요구였다

가장 오래 남은 건 두 번의 오답이다. 병렬 실행의 전체 시간이 합이 아니라 최댓값이라는 것, 그리고 60초라는 상한을 내가 아니라 벤더 유량이 정한다는 것. 두 번 다 "줄이면 되겠지"라는 직관이 산술과 실측 앞에서 틀렸다.

그래서 구조를 바꿀 때 내가 먼저 확인하는 건 무슨 기술을 더 쓸 수 있느냐가 아니라 요구가 어떤 시간 단위로 바뀌었느냐다. 네 번 다 그 순서였다.
