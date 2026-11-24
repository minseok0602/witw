# 설계 및 측정 계약

## 고정 환경

독립 변인은 전략과 테마별 기사 분포다. Core·Worker·목표 Task 수는 바꾸지 않는다. 각 Application을 독립 실행하며 Executor 2개×2 Core, task.cpus=1, shuffle/default parallelism=4, dynamic allocation/speculation/AQE=false로 고정한다. 실제 ExecutorAdded·TaskContext·Stage Task 수로 확인한다.

Driver heap 1536 MiB/container 3 GiB/CPU 1, Executor heap 2 GiB/Worker container 4 GiB/CPU 2, Master 512 MiB/CPU 0.5다. 측정 컨테이너는 Spark 4.2.0, Python 3.10.12, numpy 1.26.4, scipy 1.15.3, sklearn 1.6.1, joblib 1.5.1, threadpoolctl 3.6.0, 수치 라이브러리 스레드 1이다. Java·라이브러리·이미지 ID·코드 SHA는 실행별 환경 JSON에 기록한다. EC2에서는 AMI·CPU·메모리·NTP도 추가 기록한다.

## 입력 데이터와 시드

운영 데이터를 복제하지 않은 768차원 합성 데이터다. 생성기는 `apps/data.py`다. 동일 반복 블록의 모든 전략은 입력 SHA256·기사 목록·테마 내 순서·이슈 후보 스냅샷이 같다.

|분포|A 기사 수(T1~T8)|B 기사 수(T1~T8)|
|---|---|---|
|D1|500,500,500,500,500,500,500,500|250,250,250,250,250,250,250,250|
|D2|1200,800,600,400,300,300,200,200|600,400,300,200,150,150,100,100|
|D3|3000,400,200,100,100,100,50,50|1500,200,100,50,50,50,25,25|
|D4|4000,0,0,0,0,0,0,0|2000,0,0,0,0,0,0,0|

후보 이슈 수는 200,100,50,10,150,80,30,20으로 고정한다. 반복마다 위 기사 수와 테마 ID의 대응을 시드로 재배치한다. 따라서 A의 총 비교 수와 D4의 후보 수는 반복 간 달라질 수 있다. 개선율은 같은 입력 블록의 전략끼리 계산한다.

Quick seed는 A=810000/B=910000 + scenario_index×100+rep다. Explore는 +1000000, Full은 +2000000이다. 워밍업 700000/700001, 모델 fit 610xxx, 모델 검증 620xxx, 모델 워밍업 600xxx로 분리한다. 블록 안의 전략 순서도 시드에서 무작위로 정하고 CSV에 남긴다.

## 파티셔닝 전략

A0는 초기 4파티션을 유지한다. A1은 기사 수, A2는 max(1,후보 수)×768 비용을 균등하게 배분한다. 계획용 기사 payload는 Driver로 collect하지 않는다. 입력 파티션×테마 count만으로 quota를 만들고 Executor의 prefix/ordinal로 각 기사의 목적지를 정한다. A1/A2는 같은 RDD partitionBy와 목적지 번호를 그대로 반환하는 함수를 사용한다.

B0는 RDD groupByKey 기본 hash, B1은 기사 수, B2는 독립 모델의 예상 초 단위 비용을 사용하는 LPT다. 모두 동일한 RDD 표현·API·후처리를 사용한다. 동일 테마는 분할하지 않고 여러 테마의 동거를 허용한다. 계획한 목적지와 실제 TaskContext partition ID를 비교한다. D4에서는 3개 빈 Task가 발생하며 하나의 HDBSCAN을 4 Core에 분할하지 않는다.

AS_IS는 테마별 sort/limit/collect 후 Driver 순차 실행, CONTROL은 일괄 collect 후 같은 Driver 순차 실행이다. 720회 주 비교와 별도로 측정한다. Core·Task 확장 실험은 이번 범위에서 제외한다.

## 원본 연산과 정합성

매칭은 원본 pure Python cosine, summary embedding 우선, 임계값 0.93, 낮은 issue ID를 선택하는 동점 정책을 유지한다. 원본 Kafka 변환 후 9개 열을 유지해 Shuffle payload를 축소하지 않는다. 기사 ID, 출력 topic/key, JSON payload, 미분류, 최고 유사도, 누락·중복을 비교한다. 최고값 계측 래퍼는 모든 군에 동일하게 적용하고 timer에 포함한다.

군집화는 원본 `cluster_articles`를 호출한다. min_cluster_size=8, min_samples=4, confidence=0.80, centroid similarity=0.92, 대표 4, 테마당 최대 2,000건이다. 입력을 (unclassified_at,article_id)로 정렬한다. 선택 입력 ID·순서, 군집 수·멤버 ID 집합, 미분류, 대표 ID, centroid, confidence를 비교하며 임의 label 번호는 사용하지 않는다. 사전에 정한 정규화는 유사도 소수 12자리, centroid/confidence 소수 10자리 반올림이다. 실패 후 허용 범위를 확대하지 않는다.

## 측정 경계와 보정

입력 생성→공통 초기 4파티션→eager localCheckpoint→공통 테마 집계까지 준비 구간이다. 주 timer는 전략별 추가 집계·계획부터 Shuffle·원본 연산·동일 방식의 최종 collect까지다. 독립 원본 참조 실행·결과 정규화·CSV 저장은 timer 밖이며 비용 추정용 전체 실행 시간에는 포함한다. Kafka/PG/HDFS/LLM I/O는 A/B에서 제외한다.

모든 전략에 2회 소규모 워밍업 블록을 적용하고 통계에서 제외한다. 비용 모델도 실제 Executor의 4개 동시 Task로 전용 워밍업 8 Task를 수행한 후 측정한다. 9개 크기×2반복×4Task×fit/독립 검증=144개 보정 표본을 사용한다. 선형, 제곱+절편, 크기별 실측 보간을 독립 검증 MAE(초)로 비교한다. 제곱 모델 선택은 HDBSCAN 복잡도에 대한 증명이 아니다. 모델은 합성 밀도·CPU·구현에 의존하며 EC2에서 다시 보정한다. 본실험 holdout으로 모델을 재조정하지 않는다.

## 반복·통계·실패

Quick은 조건별 1회, Explore 3회, Full 30회다. Full A360+B360=720회이며 Driver 기준군 240회는 별도다. p50/p95/표준편차, 중앙값 기준 throughput/speedup, paired 평균 차이와 10,000회 bootstrap 95% 구간을 산출한다. 소표본 p95·신뢰구간으로 승자를 확정하지 않는다. 잠정 채택 기준은 30표본 이상, p50 10% 이상 개선, paired 구간 하한>0이며, 통합 전략은 분포 빈도·구현 복잡도까지 검토한다. 개선이 없으면 기본 방식을 유지한다.

실패·재시도·Executor loss·OOM을 기록하고 해당 실행을 fail로 처리한다. 실패 결과를 삭제하거나 정상 행만 선택해 주 분석에 섞지 않는다. 불완전한 전체 실행을 제외한 이유를 남긴다. localCheckpoint는 Executor 소실 시 재계산할 수 없으므로 새 run ID로 다시 측정한다.

## 메트릭 해석의 한계

Task CV는 실제 연산 Stage의 빈 Task까지 포함한다. AS_IS/CONTROL에는 Executor 군집화 Stage가 없으므로 CSV target CV=0은 비해당을 의미하며 균형이 좋다는 뜻이 아니다. 그래프에서는 두 기준군의 CV를 표시하지 않는다.

Event Log로 Job/Stage/Task, Shuffle, fetch wait, JVM de/serialize, GC, spill을 추출한다. Scheduler residual은 내부 scheduler CPU 직접 측정치가 아니다. 병렬 Task 시간 합을 wall에서 빼지 않는다. Python kernel 시간과 JVM Task CPU를 구분한다. 250ms 자원 샘플은 짧은 처리·peak를 놓칠 수 있고, Worker CPU에는 daemon도 포함된다. Python RSS는 해당 프로세스의 누적 peak이므로 시도별 전용 peak로 해석하지 않는다.

합성 초기 4파티션은 article_id의 Spark xxhash64를 사용해 운영 Kafka murmur2/6파티션을 완전히 재현하지 않는다. 로컬 NIC는 컨테이너 간 전송량이다. 실제 서버 간 네트워크·도착 배치 효과는 EC2/C에서 검증해야 한다. 고정 4 Core 비교이므로 1 Core 기준 강한 확장성의 Parallel Efficiency는 측정하지 않았다.
