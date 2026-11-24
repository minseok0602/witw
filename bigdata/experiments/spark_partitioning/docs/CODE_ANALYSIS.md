# 운영 구현과 실험 경계

분석 대상 파일의 SHA-256은 `../source_audit.json`에 고정했다. 기존 사용자 수정 사항은 보존했고 운영 코드·운영 데이터·기존 Docker Compose 서비스에는 쓰지 않았다. 기존 실험 자료는 `../../clustering_benchmark/`에 그대로 보존했다.

## Application A

`enriched_article_stream.py`는 Kafka enriched 메시지를 `_transform_kafka_partition`으로 검증·변환하고, `article_event.py`의 summary_embedding 우선 / 없으면 article_embedding 정책을 적용한다. ARTICLE_SCHEMA의 9개 컬럼을 유지한다. ARTICLE_UPSERT Kafka 발행 후 현재 배치의 theme_id를 수집하고 최근 72시간의 이슈 후보를 PostgreSQL에서 조회하여 Broadcast한다. `_match_partition`은 `issue_matching.build_issue_result`를 호출한다.

`find_best_issue`는 동일 테마 후보를 전부 순회한다. 실제 `cosine_similarity`는 벡터별 float 변환·dot product·양쪽 norm을 매 비교마다 수행한다. 미리 정규화한 벡터를 dot product로 바꾸거나 NumPy로 벡터화하지 않았다. 임계값은 0.93이고, 최고 유사도 동점이면 작은 issue_id를 선택한다. 원본 이벤트 ID 생성 함수도 유지한다.

실험은 `_transform_kafka_partition`으로 만든 원본 9컬럼 DataFrame의 준비 이후 시작한다. 입력 JSON 파싱, ARTICLE_UPSERT Kafka write, JDBC 조회와 Broadcast 생성은 공통 준비 구간이다. 고정된 8테마/640개 후보 snapshot을 저장하고 같은 snapshot을 모든 전략에 사용한다. 운영은 활성 테마만 조회하지만 실험은 후보 snapshot을 고정하여 조회 변동을 제거했다. 각 기사에 전달되는 후보 목록과 매칭 비즈니스 연산은 동일하다.

최고 유사도는 원래 함수가 미할당 기사에 반환하지 않으므로, 실험 Python 프로세스 안에서 원본 cosine 함수를 감싸 반환값을 계측한다. 원본 계산과 반환은 그대로 수행한다. 이 동일 계측을 A0/A1/A2에 모두 적용하며 비용도 측정에 포함한다. 디스크의 운영 모듈은 수정하지 않는다.

A0는 준비된 입력 4파티션을 유지한다. A1/A2는 theme_id 열만 읽어 입력 파티션별 theme count를 수집한다(최대 4×8개 집계). 기사 ID·임베딩·본문은 계획 생성을 위해 Driver로 collect하지 않는다. A1은 전역 ordinal의 modulo로 기사 수를 균등 배분한다. A2는 `max(1, 후보 수)×768`을 사용해 집계 count만으로 파티션별 quota를 계산하고, source partition별 테마 prefix와 로컬 순번으로 각 기사의 목적지를 Executor에서 결정한다. 추가 집계·계획·Python PairRDD Shuffle·결과 수집을 모두 timer 안에 포함한다.

## Application B

`unclassified_issue_stream.py`는 pending/state 및 trigger를 먼저 계산한 후, 대상 테마마다 `(unclassified_at, article_id)` 순서로 최대 2,000개를 collect하여 Driver의 `cluster_articles`를 호출한다. 실제 stream 인자는 min_cluster_size=8, min_samples=4, membership=0.80, centroid similarity=0.92, 대표 기사 4다. 함수 기본 인자와 stream의 명시 인자는 구분했다.

원본 함수는 sklearn HDBSCAN, Euclidean, leaf, allow_single_cluster=False다. 군집 멤버 confidence 및 centroid 경계 필터와 대표 기사 선택을 포함한 원본 함수를 그대로 사용한다. 같은 테마를 쪼개지 않는다. B0/B1/B2 모두 Python PairRDD groupByKey와 같은 tuple 데이터 표현, 같은 mapPartitions 후처리를 사용한다. B0는 RDD 기본 portable hash, B1/B2는 목적지 번호를 반환하는 partitionFunc를 쓴다. 목적지를 SQL hash에 다시 넣지 않는다. 실제 TaskContext partition ID와 계획을 비교한다.

AS_IS/CONTROL은 별도 기준이다. AS_IS는 운영의 테마별 sort/limit/collect, CONTROL은 한 번 collect한 다음 Driver에서 동일 sort/limit/원본 kernel이다. 주 비교는 B0/B1/B2이며 720회에 두 Driver 기준군을 합산하지 않는다.

pending_counts 집계는 원래 stream의 공통 작업이므로 timer 밖에서 따로 기록한다. B1/B2의 계획 계산과 Shuffle은 timer 안이다. 주 실험의 전체 2,000건과 테마별 상한을 구분하며 모든 비교군의 선택 ID 목록까지 비교한다.

## 입력·API·운영과 다른 범위

운영 Kafka topic 생성 설정은 raw/enriched/unclassified 각 6파티션, event 3파티션이다. 주 실험은 공통 초기 4파티션으로 시작하며 Kafka 6파티션 결과로 부르지 않는다. 실제 6파티션 source를 사용한 baseline은 별도 부가 실험 대상이다. NLP producer의 실제 enriched key는 article_id다. 주 실험의 article_id 기반 초기 4파티션은 이를 모사하지만 Kafka murmur2 해시를 정확히 재현한 것은 아니다.

직렬화된 Python parallelize 입력 리터럴이 캐시 이후 Task에 남는 이전 실험의 문제를 피하기 위해 준비 단계에서 eager localCheckpoint로 lineage를 끊는다. 이 준비 시간은 공통 비용에 기록한다. checkpoint 저장 RDD만 case 종료 후 해제한다. 다중 EC2에서 공유 local 파일시스템을 요구하지 않는다. localCheckpoint는 executor 소실 시 재계산 불가하므로 executor loss를 검증 실패로 처리하고 자동으로 표본을 지우거나 대체하지 않는다.

A0는 추가 Shuffle이 없고 A1/A2는 Python PairRDD Shuffle을 추가한다. 이 API·직렬화 경로 차이는 설계상 추가 비용이며 감추지 않는다. B0/B1/B2는 같은 RDD API와 표현이다. 모든 전략은 결과를 같은 방식으로 완전히 collect하여 lazy evaluation 누락을 방지한다.

A/B 주 실험은 외부 Kafka/JDBC/HDFS/LLM 없이 원본 연산 함수까지 비교한다. 실제 arrival rate가 아니라 이미 준비된 기사 수다. C는 A/B 독립 본실험 이후 별도 실행하며 Kafka-Spark-DB/HDFS 및 고정 LLM 응답을 포함해야 한다. 로컬 연산 결과를 C의 end-to-end 결과로 표시하지 않는다.

## 자원·계측

운영 Compose는 두 Application에 각각 spark.cores.max=1을 지정하고 worker 한 개를 2 Core/2 GiB로 실행한다. 이번 실험은 명세에 따라 worker 두 개, executor당 2 Core, 총 4 Core로 고정한다. 운영 Compose를 고쳐 실험하지 않는다. 실제 ExecutorAdded Event에서 [2,2]를 검사하며 target stage의 실제 Task 4개도 확인한다.

Driver heap 1536 MiB/container 3 GiB/CPU 1, Worker당 executor heap 2 GiB/container 4 GiB/CPU 2, master container 512 MiB/CPU 0.5다. Worker daemon은 256 MiB다. 실제 Quick Test의 실패/OOM 여부와 샘플 메모리로 실행 가능성을 확인하고 동일 조건을 유지한다.

프로젝트 Prometheus 설정의 Spark JVM source 및 servlet, 운영 streaming_metrics의 batch/lag 지표를 분석했다. 주 실험에서는 Event Log, TaskContext 및 cgroup 250ms 샘플링을 추가한다. JVM Task CPU에는 Python 시간이 빠지므로 Python kernel process_time 및 container CPU를 함께 기록한다. memory.current에는 파일 캐시가 포함되고, process-tree RSS/ru_maxrss는 공유 페이지나 이전 실행 peak를 포함할 수 있다.

Scheduler 잔여 시간은 Task duration에서 executor run/deserialize/result serialization/getting-result를 뺀 비음수 잔여값이다. Stage submission→Task launch 대기 시간도 별도로 제공하며 둘을 순수 scheduler 내부 CPU 비용으로 단정하지 않는다. 병렬 Task 합계를 wall에서 빼지 않는다. cgroup CPU idle capacity는 총 4 Core×wall에서 Worker CPU 소비를 뺀 근사치이며 250ms 보간 오차를 명시한다.

local Docker NIC counter는 컨테이너 사이 전송이다. 실제 서버 간 네트워크 수치는 EC2 node NIC sampler에서만 주장한다. JVM deserialize/serialize 지표는 Python pickle/Py4J 전 비용을 분리한 값이 아니다.
