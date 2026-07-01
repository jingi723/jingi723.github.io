---
title: "edge 개발 일지 — 이력서 3줄로 정리한 Kafka 데이터 파이프라인"
date: 2026-07-01 10:00:00 +0900
categories: [dev-log, data-pipeline]
tags: [kafka, debezium, outbox-pattern, cdc, idempotent-consumer, swm]
description: 이벤트 기반 AI 분석 모듈을 MTS에 넣는 B2B SaaS 'edge'. 재처리 가능한 Kafka 수집 파이프라인을 이력서 3줄로 정리하고, 각 줄을 설계 단위로 풀어 기록한다.
---

## 프로젝트 소개

- **edge** — 이벤트 기반 AI 분석 모듈을 증권사 MTS에 탑재할 수 있는 형태로 제공하는 B2B SaaS
- **SW마에스트로 17기**
- 역할: 백엔드 · 데이터 파이프라인 개발 (2026.06~)

이 글은 edge에서 내가 맡은 **데이터 수집 파이프라인**을 "이력서 3줄"로 먼저 압축하고,
각 줄을 실제 설계 단위로 풀어 기록하는 개발 일지다.
목표를 먼저 문장으로 못 박고, 진행하면서 채워 나가는 방식으로 적는다.

> 아직 구현이 끝난 항목보다 **설계·검증 예정** 항목이 많다. 완료형이 아니라 진행형으로 적는다.
{: .prompt-info }

## 이력서 3줄

1. **Kafka 기반 At-least-once 처리와 Idempotent Consumer를 적용한 금융 데이터 Ingestion Pipeline 구현**
   — 외부 뉴스·공시 데이터를 Kafka topic으로 수집하고, message key, partition key,
   manual offset commit, DB unique constraint, idempotent consumer, DLQ/replay를 적용해
   재처리 상황에서도 동일 데이터가 중복 반영되지 않도록 설계한다.

2. **Debezium CDC + Outbox Pattern 기반 이벤트 발행 정합성 개선**
   — 뉴스 원문 저장과 Kafka 이벤트 발행 사이의 dual-write 문제를 줄이기 위해
   `news_raw`와 `outbox_events`를 하나의 DB transaction으로 저장하고,
   Debezium CDC를 통해 outbox 변경 이벤트를 Kafka로 발행하는 구조를 구성한다.

3. **Kafka Reliability Lab을 통한 장애 상황 재현 및 복구 전략 검증**
   — worker 강제 종료, DB 저장 후 offset commit 전 장애, 외부 API timeout, poison message,
   DLQ replay, consumer lag 증가 상황을 재현하고, offset commit 전략과 idempotent consumer의
   중복·유실 방지 효과를 검증한다.

아래부터는 이 세 줄을 하나씩 설계 단위로 풀어 적는다.

---

## 1. Kafka 기반 금융 데이터 Ingestion Pipeline

외부 뉴스·공시·시장 데이터는 **중복 수집, worker 장애, 외부 API 실패, 재처리** 상황이
언제든 발생할 수 있다. 이를 전제로, 수집 데이터를 Kafka topic으로 전달하고
downstream worker가 비동기적으로 처리하는 구조를 잡았다.

```
External Source → Collector → Kafka Topic
    → Crawling / Parsing Worker → Analysis Request Worker → Analysis Result DB
    (반복 실패 메시지) → DLQ → Replay Worker → 재투입
```

핵심은 Kafka 사용 그 자체가 아니라 **재처리 가능한 구조**다. 정리하면 이렇다.

- **message key / partition key** 로 처리 순서와 흐름을 제어한다.
- DB 저장 이후 **manual offset commit** 을 수행한다 — 처리가 끝나기 전에 offset을
  올리지 않는다.
- 동일 메시지가 다시 처리되더라도 **DB unique constraint** 와 **idempotent consumer** 로
  최종 결과가 중복 반영되지 않게 한다.
- 반복 실패 메시지는 **DLQ** 로 이동시키고, **replay worker** 로 재처리한다.

> "정확히 한 번(exactly-once)"을 애플리케이션에서 직접 보장하기보다,
> at-least-once로 받아들이고 **idempotency로 중복을 흡수**하는 쪽을 택했다.
{: .prompt-tip }

---

## 2. Debezium CDC + Outbox Pattern

뉴스 원문을 DB에 저장한 뒤 Kafka 이벤트를 **직접 발행**하는 구조에서는,
DB 저장은 성공했는데 Kafka 발행이 실패하는 **dual-write 문제**가 생긴다.
반대로 발행만 성공하고 저장이 실패하는 경우도 마찬가지다.
이 틈을 줄이기 위해 **Transactional Outbox Pattern** 을 적용한다.

```
Collector Server
    → (1 DB transaction) news_raw + outbox_events 저장
    → Debezium CDC (outbox_events 변경 캡처)
    → Kafka Topic → Downstream Worker
```

- `news_raw` 와 `outbox_events` 를 **하나의 DB transaction** 으로 저장한다.
  둘 다 커밋되거나, 둘 다 롤백된다.
- **Debezium CDC** 가 `outbox_events` 테이블의 변경사항을 감지해 Kafka로 전달한다.

이렇게 하면 애플리케이션 코드에서 DB write와 Kafka publish를 **동시에 성공시켜야 하는 부담**을
덜 수 있다. 저장된 데이터와 downstream 이벤트 흐름 사이의 정합성을 구조적으로 끌어올리는 게 목적이다.

---

## 3. Kafka Reliability Lab

Kafka Reliability Lab은 edge의 서비스 기능이 아니라,
**수집 파이프라인의 장애 상황을 재현·검증하기 위한 실험 환경**이다.
"이렇게 하면 안전하다"를 말로 주장하는 대신, 깨지는 조건을 직접 만들어 확인한다.

주요 실험은 다음과 같다.

- worker 처리 중 강제 종료
- DB 저장 후 offset commit 전 장애
- offset commit 후 DB 저장 실패
- 외부 API timeout
- poison message 입력
- DLQ replay
- consumer lag 증가

이 실험의 목적은 두 가지다.

1. **offset commit 위치에 따라** 중복 또는 유실이 발생하는 조건을 눈으로 확인한다.
   - `② DB 저장` 후 `③ offset commit` 전에 죽으면 → 재처리 시 **중복**
   - `③ offset commit` 후 `② DB 저장` 이 실패하면 → **유실**
2. **idempotent consumer, DB unique constraint, DLQ/replay** 구조가 복구에
   실제로 어떤 역할을 하는지 검증한다.

즉 1번·2번 항목에서 "이렇게 설계했다"고 적은 방어 장치들이,
실제 장애 상황에서 **정말 중복·유실을 막아 주는지**를 이 Lab에서 반증하듯 확인하는 셈이다.

---

## 앞으로

지금은 세 줄 모두 "이렇게 설계한다 / 검증한다"는 **계획에 가깝다**.
앞으로 dev-log를 이어 가며

- 각 항목의 실제 구현 코드와 설정,
- Reliability Lab에서 나온 **재현 로그와 수치**,
- 설계대로 안 됐던 부분과 그 이유

를 채워 넣을 생각이다. 완료된 자랑보다, **막힌 지점과 그때의 판단**을 남기는 걸 우선한다.
