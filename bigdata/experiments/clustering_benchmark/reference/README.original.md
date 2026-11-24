# WITW — 적응형 HDBSCAN 실행 전략 벤치마크

## 범위와 상태
- 실제 수행: `pilot.py` 75회 (학습 30회, 독립 검증 45회). Python 3.13, scikit-learn 1.8.0, CPU quota 4 vCPU, 메모리 4 GiB, 합성 768차원 데이터. `pilot_kernel.py`는 프로젝트의 수치 연산 부분을 옮긴 것으로 원본 파일 전체가 아니다. `pilot_results.csv`가 원시 측정값이다.
- Spark 본실험: `benchmark.py` 및 `event_metrics.py` 제공. 이 환경에는 PySpark가 없어서 실행 검증하지 못했다. 컴파일 검사만 수행. Spark 결과를 임의로 만들지 않았다.
- GitHub develop의 `bigdata/spark/apps/issue_clustering.py` (blob 1c34ad0447569e9ce3c24697bf6e2dbc7a44e81d), `unclassified_issue_stream.py` (blob b93ae1ac37b2e773bcaa4ecd19fb08d4e60abb8d) 기준. 실제 본실험은 저장소의 원본 함수를 import 한다.
- 운영 코드, Kafka, PostgreSQL, HDFS, 체크포인트에는 쓰기 작업을 하지 않는다. LLM 및 API 호출 없음.

## 연구 질문
1. 같은 입력과 자원에서 테마별 군집화의 실행 위치를 바꾸면 얼마나 빨라지는가?
2. 군집화 연산의 감소분보다 Spark Job/Shuffle/직렬화 비용이 커지는 조건은?
3. 학습 데이터와 분리한 검증 데이터에서 적응형 정책이 느린 실행 전략을 얼마나 회피하는가?

## 실행 전략
- AS-IS: 캐시된 pending에서 테마별 `filter -> orderBy -> limit(2000) -> collect` 이후 Driver에서 `cluster_articles` 순차 수행. 실제 코드의 LLM 전 구간만 재현.
- TO-BE 1: eligible pending을 theme_id 기준 `groupByKey`로 shuffle, 각 그룹을 정렬/상한 적용 후 Executor에서 동일 함수를 수행하고 후보 전체를 collect. 테마 그룹 수와 Spark Task 수가 반드시 같지 않음. 한 테마를 임의 분할하지 않음.
- TO-BE 2: 총 기사수, eligible 테마 수, 최대 테마 기사수, 최대 테마 비중으로 보정 세트의 가까운 프로필을 찾음. 해당 프로필에서 TO-BE 1 중앙값이 AS-IS 대비 10% 이상 감소한 경우만 TO-BE 1 선택. 이 정책은 비교용이며 운영 적정 임계값으로 확정하지 않는다. 코어 구성별 별도 보정. 검증 결과를 정책 결정에 사용하지 않는다.

## 실행 방법 (저장소의 Spark/Python/ML 환경에서)
압축 해제한 디렉터리를 `/path/to/bench`로 가정. 저장소 루트에서 실행. 아래 명령은 bash 예시.

```bash
export OMP_NUM_THREADS=1 OPENBLAS_NUM_THREADS=1 MKL_NUM_THREADS=1 NUMEXPR_NUM_THREADS=1
BENCH=/path/to/bench
spark-submit --master 'local[4]' \
  "$BENCH/benchmark.py" --source "$PWD/bigdata/spark/apps" \
  --cores 4 --quick --output "$BENCH/quick-4"
python "$BENCH/event_metrics.py" "$BENCH/quick-4"
```

Smoke 통과 후 본실험: 각 실행마다 새 output 디렉터리, 동일한 머신/라이브러리/메모리 제한. Spark 및 sklearn은 프로젝트 버전으로 고정. `local[N]`은 단일 머신이며 물리적 수평 확장 아님. 1/2/4 Core별 별도 결과 및 정책.

```bash
for n in 1 2 4; do
  spark-submit --master "local[$n]" \
    "$BENCH/benchmark.py" --source "$PWD/bigdata/spark/apps" \
    --cores "$n" --reps 10 --seeds 3 --output "$BENCH/full-$n"
  python "$BENCH/event_metrics.py" "$BENCH/full-$n"
done
```

Standalone에서도 실행 가능하도록 구성했지만 운영 클러스터 대신 별도 실험 클러스터를 사용하고 각 Worker에 동일 Python 및 ML 패키지를 설치할 것. 예: spark-submit --master spark://... --deploy-mode client --conf spark.cores.max=4 --conf spark.executor.cores=2 ... --cores 4. Driver 및 executor 메모리, host CPU quota, 다른 프로세스 부하를 기록. 물리적 분산 비용은 별도 단계.

## 통제 조건
- 동일 seed별 동일 기사 ID, 임베딩, 테마 분포, 입력 순서 및 상한 2000, 768차원. pending은 모든 테마가 이미 군집화 대상이라고 가정. 실제 유입량이 아니라 누적 pending 크기. 동일 cache 입력을 각 전략에 재사용하고 매 실행마다 재연산. 학습/검증 seed 및 profile 분리. 실행 순서 seed 기반 섞기, warmup 제외.
- HDBSCAN min_cluster_size=8, min_samples=4, membership=.80, centroid=.92, 대표 기사 4, 동일 거리/selection 설정. 원본 코드 import. LLM, Kafka, 상태 읽기/쓰기, DB 조회, Trigger 주기 제외. Driver/Executor Python 수치 연산 스레드 1, AQE/동적 할당/speculation 비활성화 (본실험 이후 운영 설정 민감도 확인).
- 균등/편향 paired: holdout 1200건 동일 총량에서 [300,300,300,300] vs [1080,40,40,40]. 소량 [32]*4, 단일 600, 다수 소형 [40]*12, 혼합 [400,200,100,50,25,25]. 추가 real snapshot 리플레이 전까지 운영 대표성 주장 금지.
- AS-IS와 TO-BE에서 클러스터 기사 ID, 대표 기사 순서, centroid 및 confidence(소수점 10자리) 해시 일치 검사. 불일치 시 실패 종료.
- 모든 모드는 동일 배정 자원을 보유하며 순차 처리에서 자원이 놀 수 있음. 이는 의도한 실행 전략 차이. CPU 시간과 실제 할당 자원 함께 보고.

## 결과 및 오버헤드
- `runs.csv`: raw per-run wall, kernel sum/max/CPU time, routing, digest, clusters, assigned. wall은 pending 캐시 준비 이후부터 후보 수집 완료까지. `kernel_sum_s`는 병렬 구간의 합이므로 wall에서 빼면 안 됨.
- `summary.csv`: 독립 검증 median, p95, speedup, reduction, policy choice. 기본 반복 10샘플/조건, 본실험 명령 30샘플/조건. p95는 소표본 추정치; seed별 paired ratio와 분산도 같이 확인.
- `overhead_probe.csv`: HDBSCAN 없이 같은 입력 선택/Shuffle/collect 경로 실행. 반환을 기사 ID로 축소했기 때문에 실제 후보 반환 비용까지 동일한 것은 아님. 구조적 오버헤드 비교용 보조 측정이며 full wall 차이를 그대로 오버헤드라고 단정하지 않는다.
- `event_metrics.csv`: group별 Spark Job 수, Task 수/실패, Task duration sum/median/max, executor CPU/run, shuffle bytes, deserialize/serialize, GC/spill. 값은 Task 합계이지 elapsed가 아니다. Spark UI/Event Log에서 stage critical path와 scheduler delay를 확인한다. Python CPU time은 kernel에서 별도 측정하며 JVM CPU와 중복/누락 범위에 유의. RSS/peak 메모리와 CPU utilization은 host/container 모니터링으로 추가 수집.
- `environment.json`, `policy.json`, `spark-events/` 보존. 보정과 검증 결과를 섞지 않음. 정합성 해시가 같지 않으면 성능 비교 실패. 적응형이 항상 승리한다고 가정하지 않음; 선택 오류, 속도 손해, p95도 보고.

## 예비 실험 재현
```bash
cd /path/to/bench
python pilot.py
```
예비 실험은 이미 그룹화된 데이터에 대해 순차 vs 4-process pool을 비교한다. Spark Job/Shuffle 비용 없음. warm pool 사용, IPC/직렬화/프로세스 스케줄링 포함. 단일 테마 및 편향 분포에서는 분산의 순이익이 없었으나 Spark에서도 같다는 결론은 낼 수 없다. `pilot_results.csv`의 seed는 pilot.py에서 확인 가능. 3회 반복은 탐색용이며 통계적 확정이 아니다.

## 다음 검증
1. 실제 Spark isolated 벤치마크 완료 및 event log 분석.
2. 익명화한 실제 pending 스냅샷 리플레이, 분포별 다수 seed. 유입에 따른 trigger/72시간 상태와 LLM은 고정 응답 fixture로 별도 end-to-end 실험. 실제 유입 분포와 발생 빈도로 가중한 결과 계산.
3. 결과가 유효할 때만 별도 실험 브랜치에서 TO-BE 통합. 운영 적용 전 LLM 순서, cooldown, attempt state, pending 갱신, 장애 재처리 및 메모리 상한 검증. Spark AS-IS 최적화(일괄 collect + Driver 순차)를 추가 대조군으로 두어 반복 collect 제거 효과와 병렬화 효과 분리.
