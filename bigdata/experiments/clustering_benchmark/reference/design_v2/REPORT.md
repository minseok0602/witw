# HDBSCAN 실행 전략 비교 실험

상태: 보완한 Quick Test 통과. Executor 1·2·4 Core 본실험을 순차 수행 중이다. 이 문서는 본실험 종료 후 최종 측정값으로 갱신한다. 본실험의 성능 결론은 아직 확정하지 않았다.

## 검증한 범위

현재 작업 트리의 운영 `cluster_articles` 함수를 Driver와 Executor에서 직접 사용했다. `source_audit.json`에 남긴 운영 파일 해시와 스트림 인자를 기준으로 실험하며 운영 파일은 변경하지 않았다. 기존 운영 변경 사항은 실험 시작 시점의 사용자 작업으로 보존했다.

운영 함수는 sklearn.cluster.HDBSCAN, Euclidean 거리, leaf selection, allow_single_cluster=False를 사용한다. 스트림이 명시하는 최소 군집 8·min_samples 4·대표 기사 4·membership 0.80·centroid similarity 0.92를 그대로 적용했다. normalized embedding 768차원 및 테마당 2,000건 상한도 유지했다. 서로 다른 테마를 하나의 HDBSCAN으로 합치거나 한 테마를 여러 HDBSCAN으로 쪼개지 않았다.

외부 LLM·Kafka·PostgreSQL·HDFS 및 streaming state 처리 비용은 포함하지 않는다. trigger를 통과한 pending DataFrame에서 대상 기사 선택을 시작하여 후보 객체가 Driver에 반환되는 구간을 비교한다. 이 범위를 전체 운영 파이프라인 지연으로 확대 해석하지 않는다.

## Quick Test

첫 Quick Test는 후보 정합성을 통과했으나 `parallelize` 합성 입력의 큰 리터럴이 캐시 이후에도 Task 직렬화에 남았다. 이 경로는 운영 입력과 다르므로 최종 성능 집계에서 제외했다. 원시 CSV·Event Log·실행 코드와 제외 사유를 보존했다.

합성 데이터를 측정 전 로컬 실험 Parquet으로 준비하고, 이를 읽어 4파티션 DataFrame으로 캐시하도록 보완했다. 보완된 Quick Test는 성능 측정 84회, 워밍업 9회, 상한/비대상 테마 검사 3회를 수행했다. 성능 42조건마다 2회이며 정밀한 p95 평가를 위한 본실험은 아니다. 입력 준비의 파일 I/O와 cache materialization은 타이머 밖이다.

검사 결과는 `results/quick-4-parquet-v2/VALIDATED.json`에 있다. 모든 후보의 기사 membership·대표 기사 순서·centroid·confidence가 독립 Driver 기준과 일치했다. 기준 결과가 전부 비어 있지 않았으며 필터/노이즈 기사도 존재했다. [2011,25,7] 중 대상 두 테마에서 정확히 2,025건을 선택했다. Driver/Executor 라이브러리 및 운영 소스 해시가 같았고, Executor에 4 Core가 할당되었다. Docker CPU·메모리 제한도 검사했다. 측정 중 큰 Task payload 경고는 0건이었다.

기존 운영 군집화 회귀 테스트 23개와 실험 측정/정책 테스트 3개가 통과했다. Quick Test 결과를 본실험 보정이나 최종 검증 표본으로 합치지 않는다.

## 본실험 설계

AS-IS는 테마별 Spark 정렬·limit·collect 후 Driver 순차 군집화다. CONTROL은 전체 대상 기사를 한 번 collect한 뒤 같은 정렬·limit·군집화를 Driver에서 수행한다. TO-BE 1은 테마별 groupByKey와 Executor 군집화 후 전체 후보 객체를 collect한다. TO-BE 2는 보정 프로필에서 고정한 정책으로 CONTROL 또는 TO-BE 1을 선택한다.

CONTROL 비교로 반복 Job/collect를 포함한 입력 경로 최적화와 Executor 배치의 추가 효과를 구분한다. 다만 AS-IS→CONTROL은 정렬 위치도 달라지고, CONTROL→TO-BE 1도 shuffle·데이터 변환·후보 반환 경로가 달라진다. 두 차이를 각각 순수 collect 비용과 순수 병렬화 비용이라고 단정하지 않는다. 같은 Executor 구현의 1·2·4 Core 결과도 함께 본다.

프로필·Core·전략별 30회는 3개 합성 seed × seed당 10회다. Core당 보정 540회 + 검증 720회, 총 3,780회다. 모든 전략이 같은 seed별 입력을 공유하고 각 반복 블록 안에서 실행 순서를 무작위로 섞는다. 모든 고정 전략을 3회 워밍업한 기록은 최종 집계에서 제외한다. 보정 seed는 101000번대, 최종 검증 seed는 109000번대이며 Quick과도 분리했다. 보정 완료 후 정책 파일을 저장하고 검증 결과를 정책에 재입력하지 않는다.

검증 프로필은 tiny=128건/4테마, balanced=1,200건/4테마, skew=1,200건/4테마(최대 90%), single=1,200건/1테마, many=1,200건/12테마, large=6,400건/8테마다. balanced/skew는 총량과 테마 수를 고정해 편향을 비교한다. single/balanced/many는 총량을 고정해 테마 수를 비교한다. tiny/balanced는 4테마에서 규모를 비교한다. large는 규모와 테마 수가 모두 달라지는 추가 조건이다.

한 Docker Desktop/WSL2 VM에 Master·Driver·Worker를 별도 컨테이너로 배치했다. Worker 하나와 Executor 하나를 사용하며 Executor Python Task가 여러 Core에서 병렬 실행된다. 여러 물리 서버에 걸친 확장 실험은 아니다. Driver CPU 1 Core·3 GiB, Worker CPU p Core·6 GiB, Master 0.5 Core·512 MiB를 모든 전략에 동일하게 배정한다. Executor heap 3 GiB, Driver heap 1536 MiB다. p는 1·2·4이며 총 배정 CPU는 p+1.5다.

Docker VM은 22 logical CPU, 16,506,863,616 bytes RAM을 노출한다. Linux kernel 6.18.33.2-microsoft-standard-WSL2, Docker Engine 29.6.1, Java Temurin 21.0.11, Spark 4.2.0, Python 3.10.12, NumPy 1.26.4, sklearn 1.6.1, SciPy 1.15.3이다. 프로젝트 Dockerfile의 수치 라이브러리 직접 의존성을 사용하고 전이 의존성도 고정했다. 로컬 프로젝트 이미지 캐시에 NumPy가 누락되어 실험 이미지에서 보완한 사실을 기록했다.

## 지표의 해석과 한계

wall p50/p95는 캐시 준비 이후부터 후보 객체 반환까지다. articles/s는 선택 기사 수 / wall이다. 실제 도착률이나 end-to-end pipeline throughput이 아니다. Kernel 합계는 Python 변환·정렬·HDBSCAN·후처리를 포함하며 병렬 Task들의 시간을 더한 값이다. 이 합계를 wall에서 빼서 오버헤드를 계산하지 않는다.

Spark Event Log의 Job/Stage 시간 구간, Task 수·duration·부하 비율/CV, shuffle, GC/spill, JVM 역직렬화/결과 직렬화를 따로 집계한다. Task 합계 시간은 critical path elapsed와 다르다. Stage별 부하 불균형과 Job/Stage 시간 구간의 합집합도 제공한다. JVM deserialize/serialize 지표만으로 Python pickle·Py4J·소켓 전송 비용을 완전히 분리할 수 없다. scheduler residual도 순수 scheduler 비용이라고 해석하지 않는다. ms 단위 0은 비용 부재를 뜻하지 않는다.

Driver와 Worker cgroup CPU 누적 시간 및 memory.current·anon을 250ms 간격으로 기록한다. Worker 값은 JVM·Python·Worker daemon·sampler를 포함한다. 메모리에는 파일 캐시와 이전 실행의 유지 메모리가 포함될 수 있어 전략 자체의 독립 peak RSS가 아니다. 짧은 실행의 CPU 비율은 표본 간 선형 보간 오차가 상대적으로 크다. JVM CPU에는 Python CPU가 빠지므로 kernel CPU와 cgroup CPU를 함께 확인한다.

No-clustering probe는 선택·collect·shuffle 경로를 유지하지만 기사 ID만 반환한다. 실제 후보 객체의 반환 비용과 달라 구조적 오버헤드의 보조 지표다. 과거 multiprocessing pilot은 Spark 비용을 포함하지 않아 새 결과와 합치지 않는다.

현재 groupByKey 파티션 수는 Executor Core 수에 맞춘다. 입력 캐시 파티션은 모든 Core에서 4개로 고정한다. Core 간 strong scaling 수치에는 병렬도와 reduce 파티션 배치 변화가 함께 포함된다. 각 Core 세션은 순차 실행하므로 세션 간 열·OS 부하 변화도 완전히 제거하지 못한다. 전략들은 같은 세션 안에서 무작위 교차 배치했다.

적응형 정책은 capped 총량·테마 수·최대 테마 크기의 log1p와 최대 테마 비중을 사용한다. 보정 feature 표준편차로 정규화해 가장 가까운 프로필을 찾고, Executor p50이 CONTROL보다 10% 이상 빠른 경우에만 Executor를 선택한다. 10%는 사전에 정한 margin이며 통계적 유의성을 뜻하지 않는다. 정책은 AS-IS를 선택 후보에 포함하지 않으므로 AS-IS가 더 빠른 경우를 놓칠 수 있고, 메모리 여유·군집 밀도·실제 후보 수·노드 이질성도 고려하지 않는다. 관측한 holdout 성능과 선택 오류를 그대로 보고하며 사후에 정책을 튜닝하지 않는다.

합성 seed는 세 개다. seed 안의 10회 반복은 실행 시간 변동을 보여주지만 독립적인 실제 뉴스 데이터 30개를 의미하지 않는다. p95와 paired ratio 및 seed별 결과를 함께 제공하고 통계적 유의성이나 운영 분포 일반화를 강하게 주장하지 않는다. 익명화한 실제 pending snapshot과 다중 물리 노드 검증 전에는 운영 정책으로 확정할 수 없다.

## 결과물 위치

실행·환경·산출물 설명은 `README.md`, 소스 감사는 `source_audit.json`에 있다. 보완 Quick 결과는 `results/quick-4-parquet-v2/`, Quick 그래프는 `results/quick-analysis/`다. 본실험 원시 자료는 `results/full-{1,2,4}-main-v2/`에 생성한다. 최종 통합 집계와 그래프는 본실험 완료 후 `results/analysis/`에 만든다.
