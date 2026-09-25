---
title: "Airflow 구성 방식과 AWS 배포 방식 비교"
date: 2026-09-12 09:00:00 +0900
categories: [data-pipeline, 설계]
tags: [airflow, aws, architecture, orchestration]
image:
  path: /assets/img/posts/airflow-aws-deployment/thumbnail.png
---

Apache Airflow는 여러 작업을 정해진 순서와 조건에 따라 실행하고, 실행 상태를 기록하는 도구다. 데이터를 수집한 뒤 정제하고, 정제가 끝나면 집계하는 흐름을 DAG로 정의한다. 실행 중 실패한 작업을 확인하거나 과거 데이터를 다시 처리하는 것도 이 흐름 안에서 관리한다.

AWS에서는 EC2에 직접 설치하거나, ECS·EKS에서 컨테이너로 운영하거나, 관리형 서비스인 MWAA를 사용할 수 있다. 선택에 따라 서버 관리, 장애 대응, 비용 구조가 달라진다.

비교할 때 먼저 구분할 것은 **Airflow 자체를 운영하는 일과 실제 데이터를 처리하는 일**이다. 두 부분을 같은 서버에서 실행할 수도, 서로 다른 서비스에 맡길 수도 있다. 이 구분부터 이해하면 여러 배포 방식이 어떤 문제를 해결하는지 보인다.

## Airflow를 관리하는 부분과 데이터를 처리하는 부분을 나눈다

### 기본 구조 — 일을 배정하는 쪽과 실행하는 쪽이 있다

설명용으로 매일 데이터를 수집하고 정제하는 작업을 생각해보자. 평소에는 작은 서버로 충분하지만, 과거 한 달 치를 다시 처리하는 백필에는 많은 메모리가 필요할 수 있다. 이때 전체 Airflow를 큰 서버로 옮기는 방법도 있지만, 무거운 정제 작업만 별도 서버에서 실행하는 방법도 있다.

Airflow 안에서도 역할이 나뉜다. **Scheduler**는 실행 시점과 앞 작업의 완료 여부를 보고 다음 작업을 배정한다. **Worker**는 배정받은 작업의 코드를 실행하는 쪽이다. 실행 기록은 metadata DB에 저장되며, 사용자는 UI에서 진행 상태와 실패 내역을 확인한다.

DAG 코드를 읽는 DAG processor, UI와 API를 제공하는 API server, 외부 조건을 기다리는 triggerer도 있다. 핵심은 실행을 관리하는 부분과 실제 실행 자원이 분리될 수 있다는 점이다.

배포할 때는 DB와 파일도 생각해야 한다. 서버를 교체해도 DAG 코드, 실행 기록과 로그를 다시 사용할 수 있도록 보관·전달 방법을 정한다.

### Executor와 operator — 어디서 실행할지와 무엇을 할지를 정한다

**Executor**는 Airflow 태스크를 어떤 실행 환경으로 보낼지 정하는 방식이다. `LocalExecutor`는 scheduler가 있는 서버에서 태스크를 실행하고, `CeleryExecutor`는 여러 worker에 실행을 나눈다. `KubernetesExecutor`는 태스크마다 Kubernetes의 실행 단위인 Pod를 사용한다.

**Operator**는 개별 태스크가 하는 일을 정의한다. Python 함수를 실행하는 operator도 있고, ECS에 컨테이너 실행을 요청하는 operator도 있다. 따라서 Airflow 태스크 자체는 작은 서버에서 돌면서, 그 태스크가 요청한 데이터 처리는 별도의 큰 컨테이너에서 실행할 수 있다.

![EC2의 LocalExecutor가 실행한 EcsRunTaskOperator에서 외부 ECS 업무 컨테이너를 제출하는 설명용 구성](/assets/img/posts/airflow-aws-deployment/hosting-executor-external-job.png)

*EC2에 Airflow를 두고 LocalExecutor를 사용해도 업무 컨테이너는 ECS에서 실행할 수 있다*

그림의 `EcsRunTaskOperator`는 ECS에 업무 컨테이너 실행을 요청하는 operator다. Airflow를 ECS에 배포한 구성과는 다르다. AWS Batch도 비슷하게 외부 작업을 맡길 수 있는 선택지이며, 제출된 배치 작업에 실행 자원을 배정하는 서비스다.

## EC2에서는 서버를 직접 관리하는 대신 구성을 자유롭게 정한다

### 한 대로 시작하기 — 구조가 단순하고 함께 멈출 범위가 크다

EC2는 AWS에서 빌리는 가상 서버다. 한 대에 Airflow를 직접 설치하거나 Docker Compose로 구성요소를 묶어 실행할 수 있다. 학습용이라면 DB까지 함께 두어 전체 흐름을 확인하기 쉽다.

대신 서버가 멈추면 스케줄링, UI, 실행 중인 작업과 DB가 함께 영향을 받을 수 있다. OS 패치와 Airflow 업그레이드, 디스크 관리, 백업도 직접 해야 한다. 공식 Compose 예제는 학습용 출발점이므로 그대로 운영 환경의 완성된 구성으로 받아들이면 안 된다.

운영할 때는 외부 DB와 별도 로그 저장소를 검토한다. 서버 교체는 쉬워지지만, 한 대가 멈추면 스케줄링도 중단되는 문제는 남는다.

### 여러 대로 나누기 — 실행 자원과 관리 대상을 함께 늘린다

작업이 많아지면 Airflow를 관리하는 서버와 실제 작업을 처리하는 worker 서버를 나눌 수 있다. CeleryExecutor를 사용하는 구성이 한 예다. 이때 브로커라는 중개 시스템이 실행 요청을 worker에 전달하므로, 브로커 운영도 함께 필요하다.

작업을 분산하는 만큼 서버별 DAG·Python 패키지를 맞추고 권한·네트워크를 관리하는 일도 늘어난다. 기존 EC2 운영 체계를 활용하기 좋지만 장애 대응도 별도로 설계해야 한다.

## ECS와 EKS는 컨테이너를 운영하는 기반을 제공한다

### ECS — 호스트까지 관리할지는 EC2와 Fargate에서 갈린다

ECS는 컨테이너 배포와 실행 상태를 관리하는 AWS 서비스다. Airflow를 직접 구성한다면 scheduler와 API server는 계속 실행하고 worker를 별도로 둘 수 있다. DB·DAG·로그의 보관 위치도 정한다.

**ECS on EC2**에서는 컨테이너를 올릴 EC2 서버를 팀이 관리한다. 서버 용량을 확보하고 OS를 패치하는 일까지 맡는 대신, 호스트 구성을 직접 제어할 수 있다.

**ECS on Fargate**에서는 컨테이너를 실행할 서버 관리를 AWS에 맡긴다. 팀은 컨테이너의 CPU·메모리, 이미지, 권한과 네트워크를 정한다. 서버 관리가 줄어들어도 Airflow 업그레이드와 DAG 오류 대응은 여전히 필요하다.

ECS는 지속 실행 서비스의 컨테이너가 멈췄을 때 필요한 개수를 유지하도록 대체할 수 있다. 다만 일회성으로 제출한 업무 컨테이너의 재실행이나 결과 복구까지 모두 처리해 주는 것은 아니다. 어떤 실행을 계속 유지하고 어떤 작업을 다시 시도할지 구분해야 한다.

### EKS — Kubernetes를 이미 운영한다면 활용할 수 있다

EKS는 여러 서버에 컨테이너를 배치·관리하는 Kubernetes의 운영 기반 일부를 AWS가 맡는 서비스다. Airflow를 Pod로 배치하고, 설치 구성을 묶어 제공하는 공식 Helm chart를 사용할 수 있다.

KubernetesExecutor를 선택하면 태스크마다 Pod를 만들 수 있다. 작업별 자원을 나누기 좋지만, Pod를 실행할 서버 용량과 네트워크, 저장소, Airflow의 DB와 로그도 함께 운영해야 한다.

EKS 운영 경험이 있다면 기존 배포·권한·모니터링 체계를 활용할 수 있다. Airflow 하나를 위해 Kubernetes를 처음 도입한다면 운영 범위가 크게 늘어난다.

## MWAA는 Airflow 기반 운영을 AWS에 맡기는 선택이다

### 환경형 MWAA — 익숙한 Airflow 환경을 관리형으로 운영한다

Amazon MWAA는 AWS가 제공하는 관리형 Airflow 서비스다. 환경 크기와 worker 확장 범위를 설정하고, DAG와 필요한 패키지를 배포해 사용한다. 서버와 metadata DB 같은 기반 운영을 AWS에 맡기면서 Airflow UI로 작업 상태를 확인할 수 있다.

팀은 DAG·권한·네트워크·패키지 호환성과 실패한 업무의 복구를 맡는다. 기반 관리 인력이 적지만 일반적인 Python DAG와 Airflow UI가 필요한 경우 검토할 수 있다.

### MWAA Serverless — 실행할 워크플로를 제출하고 필요한 자원을 맡긴다

MWAA Serverless는 사용자가 Airflow 환경을 상시 확보하지 않고 워크플로를 제출하는 방식이다. 서비스가 실행에 필요한 자원을 준비하므로, 간헐적으로 실행되는 작업에서 유지해야 할 기반을 줄일 수 있다.

사용 방식에는 차이가 있다. 워크플로는 YAML 형식으로 정의하고, 일반 Airflow UI 대신 AWS 콘솔과 로그로 실행을 확인한다. Python이나 셸 코드를 실행할 수도 있지만 지원되는 작성·패키징 방식에 맞춰야 하므로, 기존 Python DAG를 그대로 옮길 수 있다고 전제해서는 안 된다.

자신의 작업을 지원 형식으로 표현할 수 있는지, 일반 Airflow UI가 필요한지가 환경형 MWAA와의 선택 기준이다.

![직접 구성·MWAA·MWAA Serverless의 운영 책임 비교: 코드·패키지 호환성은 모두 사용자 책임이며 런타임 관리는 방식별로 다르다](/assets/img/posts/airflow-aws-deployment/managed-responsibility-boundaries.png)

*관리형 서비스가 실행 기반을 맡아도 워크플로의 코드·권한·결과 검증은 사용자에게 남는다*

| 구성 | 주로 직접 관리할 것 | 선택할 만한 조건 |
|---|---|---|
| 단일 EC2 | 서버와 Airflow 전체 | 학습하거나 작은 구성으로 시작한다 |
| 여러 EC2 | 서버별 역할·배포와 worker 연결 | VM 운영 체계가 있고 실행을 분산한다 |
| ECS | 컨테이너 배치와 Airflow 운영 | 기존 ECS 운영 기반을 활용한다 |
| EKS | Kubernetes 자원과 Airflow 운영 | 이미 Kubernetes 운영 역량이 있다 |
| 환경형 MWAA | DAG·권한·패키지와 환경 설정 | 일반 Airflow 사용성을 유지하며 기반 관리를 줄인다 |
| MWAA Serverless | 지원 형식에 맞춘 워크플로와 권한 | 표현 제약이 맞고 실행량이 간헐적이다 |

## 실행 자원을 늘리는 것과 실패한 일을 복구하는 것은 별개다

worker를 늘려도 scheduler의 작업 배정이나 DB의 상태 기록이 느리면 대기는 줄지 않을 수 있다. 실제 계산과 실행 관리 중 어디가 병목인지 먼저 확인한다.

구성요소를 여러 개 띄워도 같은 서버나 DB에 의존하면 함께 멈출 수 있다. 서버 분산뿐 아니라 DB 복구, DAG·로그 보존과 작업을 이어갈 절차가 필요하다.

외부 작업을 호출할 때는 실패가 더 헷갈릴 수 있다. Airflow가 ECS에 정제 작업을 제출한 직후 worker가 종료됐다고 가정해보자. Airflow 쪽 연결은 끊겼지만 ECS 컨테이너는 계속 데이터를 처리하고 있을 수 있다.

![Airflow worker 종료 후에도 ECS 작업이 계속 실행될 수 있어 기존 상태를 확인하는 설명용 흐름](/assets/img/posts/airflow-aws-deployment/worker-and-external-job-lifetimes.png)

*worker가 종료됐다는 사실만으로 외부 작업의 종료나 재제출 필요성을 판단할 수는 없다*

무조건 다시 제출하면 중복 처리할 수 있다. 기존 작업의 상태를 확인하고, 재실행해도 결과가 중복되지 않게 저장해야 한다. 서버 재시작과 업무 결과 복구는 이렇게 구분된다.

## 비용은 작업이 도는 시간과 환경을 켜둔 시간으로 나눠 본다

하루 세 번만 배치를 돌려도 EC2나 환경형 MWAA를 계속 켜두면 상시 비용이 발생한다. Fargate도 Airflow 컨테이너를 계속 실행하면 비용을 낸다. 서버 관리 여부와 유휴 시간의 과금 여부는 별개다.

외부 ECS·Batch 작업이나 Serverless처럼 실행량에 따라 자원을 사용하는 부분은 따로 계산한다. Airflow가 외부 작업을 호출했다면 Airflow 쪽 비용과 외부 작업 비용이 각각 생길 수 있다.

| 비용 구분 | 포함할 대상 |
|---|---|
| 상시 기반 | Airflow 서버·컨테이너, 환경형 MWAA, EKS 클러스터 |
| 작업 실행 | worker, 태스크별 실행 자원, 외부 ECS·Batch 작업 |
| 저장과 상태 | DB, 필요한 브로커, DAG·입출력·로그와 백업 |
| 네트워크 | 데이터 전송, 외부 통신을 위한 NAT Gateway 등 |
| 운영 시간 | 패치, 업그레이드 검증, 모니터링과 장애 대응 |

예를 들어 30일 동안 환경을 계속 켜두면 720시간이다. 하루 세 번, 한 번에 태스크 다섯 개를 각각 2분씩 실행한다면 태스크 실행 시간의 합은 15시간이다. 이는 설명용 가정이지만 **작업 실행 시간과 기반 유지 시간이 크게 다를 수 있다**는 점을 보여준다.

견적은 같은 리전과 자원 규격으로 상시·실행·저장·네트워크 비용을 합쳐 비교한다. worker 비용에 포함된 실행 시간을 다시 더하지 않고, 최소 과금 시간과 시작 시간도 반영한다.

패치와 장애 대응에 드는 시간도 함께 본다. 자원 요금이 낮아도 관리 부담이 크면 관리형을, 기존 운영 체계를 활용할 수 있다면 직접 구성을 검토할 이유가 있다.

## 팀이 이미 운영하는 기반과 작업의 성격에서 선택한다

다음은 팀의 운영 역량과 작업 요구를 기준으로 한 선택 예시다.

| 상황 | 먼저 검토할 방향 | 확인할 점 |
|---|---|---|
| 개인 프로젝트에서 수집·정제·백필을 학습한다 | 로컬 Compose, 필요하면 단일 EC2 | UI와 실행 구조를 관찰하기 쉬운가 |
| 소규모 팀이 하루 몇 차례 배치를 운영한다 | 환경형 MWAA 또는 Serverless | 기존 DAG·UI가 필요한가, 지원 형식이 맞는가 |
| 수집과 백필이 겹쳐 일시적으로 자원이 많이 든다 | 작은 Airflow와 외부 ECS·Batch 작업 | 필요한 병렬성과 재시도·결과 확인을 지원하는가 |
| 이미 ECS를 운영한다 | ECS 직접 구성 또는 ECS 작업 제출 | Airflow 자체까지 운영할 인력이 있는가 |
| EKS와 Kubernetes 운영 역량이 있다 | EKS 기반 Airflow | 기존 배포·관측 체계를 활용할 수 있는가 |

무거운 작업을 외부로 보내면 Airflow를 크게 유지하는 부담을 줄일 수 있다. 대신 이미지 배포·권한·입출력·결과 확인을 연결해야 한다. 짧은 함수 몇 개라면 관리 대상만 늘어날 수 있다.

계산 자원이 부족하면 실행 환경을, 작업 배정이 느리면 scheduler와 DB를, 유지보수가 버거우면 관리형을 검토한다. 문제가 생긴 부분이 바꿔야 할 지점도 알려준다.

## 마무리

Airflow의 AWS 배포 방식은 어디에 설치하느냐와 함께 무엇을 직접 운영할지 정하는 선택이다. EC2·ECS·EKS는 직접 구성을 설계할 기반을 제공하고, MWAA는 Airflow 기반 운영의 부담을 줄여준다.

그 위에서 무거운 작업만 외부로 보낼 수도 있다. 실행 위치를 나누면 자원을 따로 늘리기 쉬워지지만, 작업 상태와 결과를 연결하는 책임이 생긴다.

선택 기준은 필요한 실행량, 감당할 수 있는 운영 부담, 실패 후 복구할 대상이다. 이 세 가지를 함께 봐야 설치 이후에도 유지할 수 있는 구성을 고를 수 있다.

## 참고 자료

- Airflow의 구성요소와 실행 방식: [아키텍처](https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/overview.html), [Executor](https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/executor/index.html), [태스크와 실패 처리](https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/tasks.html)
- 직접 구성과 운영: [Docker Compose 빠른 시작](https://airflow.apache.org/docs/apache-airflow/stable/howto/docker-compose/index.html), [CeleryExecutor](https://airflow.apache.org/docs/apache-airflow-providers-celery/stable/celery_executor.html), [Scheduler 운영](https://airflow.apache.org/docs/apache-airflow/stable/administration-and-deployment/scheduler.html)
- ECS에서 실행하기: [ECS services](https://docs.aws.amazon.com/AmazonECS/latest/developerguide/ecs_services.html), [ECS 작업 유지관리](https://docs.aws.amazon.com/AmazonECS/latest/developerguide/task-maintenance.html), [ECS operator](https://airflow.apache.org/docs/apache-airflow-providers-amazon/stable/operators/ecs.html), [Batch operator](https://airflow.apache.org/docs/apache-airflow-providers-amazon/stable/operators/batch.html)
- Kubernetes에 배포하기: [공식 Helm chart](https://airflow.apache.org/docs/helm-chart/stable/index.html), [운영 환경 구성](https://airflow.apache.org/docs/helm-chart/stable/production-guide.html)
- 관리형 Airflow: [Amazon MWAA 개요](https://docs.aws.amazon.com/mwaa/latest/userguide/what-is-mwaa.html), [MWAA 아키텍처](https://docs.aws.amazon.com/mwaa/latest/migrationguide/mwaa-architecture.html)
- 서버리스 워크플로: [MWAA Serverless 개요](https://docs.aws.amazon.com/mwaa/latest/mwaa-serverless-userguide/what-is-mwaa-serverless.html), [Python·Bash 코드 실행](https://docs.aws.amazon.com/mwaa/latest/mwaa-serverless-userguide/operators-python-bash-detail.html)
- AWS 요금: [MWAA](https://aws.amazon.com/managed-workflows-for-apache-airflow/pricing/), [Fargate](https://aws.amazon.com/fargate/pricing/), [EKS](https://aws.amazon.com/eks/pricing/), [VPC](https://aws.amazon.com/vpc/pricing/)
