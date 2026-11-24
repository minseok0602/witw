> 보존된 이전 설계입니다. Driver/Executor 동적 전환 본실험은 사용자 요청으로 종료했으며,미완료1Core 결과는성능결론에서제외합니다. 현재실험은[상위안내](../README.md)를참조하세요. 기본 `-Mode All`을 현재 실험으로 실행하지 마세요.

# WITW 군집화 실행 전략 실험

운영 코드·데이터를 수정하지 않고, 현재 작업 트리의 `bigdata/spark/apps/issue_clustering.py`를 읽기 전용으로 직접 사용한다. 첨부 ZIP의 원문과 예비 결과는 `reference/`에 보존했다. 첨부 문서는 실험 참고 자료이며 작업 범위는 사용자 요청을 따른다.

## 실행

저장소 루트의 PowerShell에서 Docker Desktop Linux 엔진을 실행한 뒤:

```powershell
./bigdata/experiments/clustering_benchmark/run_lab.ps1 -Mode All
```

스크립트는 실험 전용 이미지를 만들고 Quick Test를 실행한다. 정합성·측정 자료 검증이 통과해야 Executor 1, 2, 4 Core 본실험을 순서대로 시작한다. 운영 Compose, EC2, Kafka, PostgreSQL, HDFS, LLM을 사용하지 않는다. 실험 네트워크는 internal이며 호스트 포트를 열지 않는다. 결과는 각 실행의 고유 디렉터리에 보존하며 덮어쓰지 않는다.

Quick만 실행하거나, 통과한 Quick 이후 본실험만 실행할 수 있다.

```powershell
./bigdata/experiments/clustering_benchmark/run_lab.ps1 -Mode Quick
./bigdata/experiments/clustering_benchmark/run_lab.ps1 -Mode Full -SkipBuild
```

본실험 기본값은 프로필·Core·전략별 30회(독립 합성 seed 3개 × seed당 10회)다. 한 seed 안의 반복을 서로 독립인 실제 기사 표본 30개로 해석하지 않는다. calibration 6프로필 × 3전략 × 30회, holdout 6프로필 × 4전략 × 30회 = Core당 1,260회, 세 Core 구성 총 3,780회이며 워밍업과 probe는 별도다.

분석은 실험 이미지와 별도인 호스트 Python 환경에서 수행한다. `requirements-analysis.txt`는 Python 3.13용이며 측정 컨테이너의 라이브러리 버전을 바꾸지 않는다.

```powershell
python -m pip install -r bigdata/experiments/clustering_benchmark/requirements-analysis.txt
python bigdata/experiments/clustering_benchmark/analyze.py <full-1-directory> <full-2-directory> <full-4-directory> --output bigdata/experiments/clustering_benchmark/results/analysis --plots
python bigdata/experiments/clustering_benchmark/finalize_report.py --analysis bigdata/experiments/clustering_benchmark/results/analysis
python -m unittest discover -s bigdata/experiments/clustering_benchmark -p test_measurement.py -v
```

## 운영 구현과 비교군

|전략|입력 선택과 실행 위치|
|---|---|
|AS-IS|테마별 filter → orderBy(unclassified_at, article_id) → limit(2000) → collect → Driver 순차 군집화|
|CONTROL|대상 테마 전체를 한 번 collect → Driver에서 테마별 같은 정렬·상한 → 순차 군집화|
|TO-BE 1|대상 테마 filter → theme_id groupByKey shuffle → Executor에서 테마별 같은 정렬·상한·군집화 → 전체 후보 객체 collect|
|TO-BE 2|기존 trigger 판정에서 이미 확보한 테마별 count로 정책 판단 → CONTROL 또는 TO-BE 1|

TO-BE 1은 테마를 임의 분할하지 않는다. Task 수와 테마 수는 다르며, 같은 파티션에 여러 테마가 배정될 수 있다. Python 정수 theme_id 해시로 배정한다. CONTROL/TO-BE 1은 상한 적용 전 대상 기사를 전송하므로 큰 pending에서 전송량·메모리가 AS-IS보다 불리할 수 있다.

현재 스트림의 명시 인자는 첨부 코드와 일치한다. min_cluster_size=8, min_samples=4, representative_count=4, membership>=0.80, centroid similarity>=0.92. 원본 함수의 HDBSCAN은 sklearn 구현, Euclidean 거리, leaf selection, allow_single_cluster=False다. 원본 함수의 기본 대표 수 8 등은 스트림에서 명시적으로 덮어쓴다. 임베딩은 실제 NLP 구현과 같은 정규화된 768차원이다. `source_audit.json`에 파라미터와 운영 파일 해시를 기록했다.

## 자원 및 측정 구간

|구성 요소|CPU quota|컨테이너 메모리|JVM/가용 메모리|
|---|---:|---:|---:|
|Master|0.5 Core|512 MiB|이미지 기본값|
|Driver|1 Core|3 GiB|heap 1536 MiB|
|Worker + Executor + Python workers|1 / 2 / 4 Core|6 GiB|Worker 4 GiB advertised, Executor heap 3 GiB|

각 Core 조건에서 모든 전략에 동일한 자원을 배정한다. 순차 전략의 미사용 Executor CPU도 배정 자원에 포함한다. 총 자원은 Executor p + Driver 1 + Master 0.5 Core이며, 보고하는 p는 Executor 가용 Core다. 한 물리 PC의 별도 컨테이너 간 Spark Standalone 실험이다. 다중 물리 서버의 네트워크 지연이나 수평 확장 실험이 아니다.

Spark 4.2.0, Python 3.10.12, NumPy 1.26.4, scikit-learn 1.6.1, SciPy 1.15.3, joblib 1.5.1, threadpoolctl 3.6.0. 프로젝트 Spark 이미지를 기반으로 하되, 로컬 캐시 이미지에 누락된 수치 라이브러리를 프로젝트 Dockerfile 버전으로 보완했다. Driver/Executor의 버전과 소스 해시를 실행 시 비교한다. BLAS/OpenMP 스레드 1, AQE·speculation·동적 할당 비활성화, Python worker 재사용, Task당 CPU 1이다.

합성 입력은 측정 전에 실험 전용 볼륨의 Parquet으로 준비한 뒤 4파티션의 DataFrame으로 캐시한다. cache count와 기준 군집화는 측정 밖에서 완료한다. 측정 종료 후 임시 fixture 파일만 삭제한다. 모든 후보를 Driver에 반환할 때까지의 wall time에 입력 선택, Spark Job/Task, 정렬, shuffle, 전송, Python 변환, HDBSCAN 및 후처리, 적응형 판단을 포함한다. 데이터 생성, fixture I/O, 캐시 준비, 버전 검사, 해시 검사, CPU/메모리 사후 집계는 제외한다.

모든 고정 전략을 3회 워밍업하고 모든 Executor Python 슬롯을 사용한다. 이 기록은 phase=warmup이며 최종 집계에서 제외한다. 워밍업은 시작 비용을 제외하기 위한 것으로 JVM JIT가 완전히 수렴했다고 단정하지 않는다. seed별 반복 블록 안에서 전략 실행 순서를 무작위로 교차 배치한다.

## 데이터와 보정/검증 분리

|프로필|보정 데이터의 테마별 기사 수|검증 데이터의 테마별 기사 수|
|---|---|---|
|tiny|25 × 4|32 × 4|
|balanced|400 × 4|300 × 4|
|skew|1450, 50, 50, 50|1080, 40, 40, 40|
|single|1600 × 1|1200 × 1|
|many|100 × 16|100 × 12|
|large|1000 × 8|800 × 8|

검증의 balanced/skew/single/many는 모두 1,200건이다. balanced/skew로 편향 효과를, single/balanced/many로 테마 수 효과를 비교한다. tiny/balanced는 같은 4테마에서 규모 차이를 비교한다. large는 더 큰 규모에 대한 추가 조건이며 테마 수도 달라지므로 규모만의 인과 효과로 해석하지 않는다.

본실험 보정 seed는 101000번대, 최종 검증 seed는 109000번대다. Quick Test의 1000/9000번대와도 분리했다. 각 seed의 기사 ID·임베딩·순서는 모든 전략과 모든 Core 조건에서 동일하며 입력 SHA-256을 검사한다. Quick의 별도 edge 조건은 [2011,25,7] 중 첫 두 테마만 대상으로 하여 상한 및 비대상 테마 제외를 검증한다.

생성 데이터는 중심별 다중 밀도 군집과 변형량이 다른 기사를 포함한다. 실제 뉴스 의미 분포를 대표한다고 주장하지 않는다. 유입량은 이미 trigger를 통과한 누적 pending 수로 대리한다. 시간당 실제 도착률, 72시간 상태, trigger 빈도, LLM cooldown 및 실패 재시도는 이 실험 밖이다.

## 적응형 정책

Core 구성마다 보정 결과만으로 정책을 고정한다. feature는 capped 총량·테마 수·최대 테마 크기의 log1p와 최대 테마 비중이다. 보정 feature 표준편차(최소 0.1)로 정규화해 가장 가까운 프로필을 찾고, 그 프로필에서 TO-BE 1 p50이 CONTROL보다 10% 이상 빠를 때만 Executor를 선택한다. 나머지는 CONTROL을 선택한다. AS-IS 대비 빠른 것만으로 병렬화 이득을 판정하지 않는다.

검증 결과는 정책에 재입력하지 않는다. `policy.json`은 보정 프로필, 각 전략의 중앙값, 선택 규칙을 보존한다. 10%는 사전에 정한 보수적 margin이며 통계적 유의성 경계가 아니다. feature에는 군집 밀도·실제 후보 수·메모리 여유·노드 이질성 등이 없으며 외삽에 약하다. 실제 운영 정책으로 확정하려면 익명화한 뉴스 snapshot과 더 많은 독립 seed가 필요하다.

## 산출물과 해석

- `runs.csv`, `inputs.csv`: 실행별 원시 시간·순서·입력/결과 해시·kernel CPU 및 wall 합계. 원본 함수 전체 후처리도 kernel에 포함한다.
- `resources-driver.csv`, `resources-worker.csv`: 250ms 간격 cgroup v2 CPU 누적 시간, throttling, memory.current, anon 메모리. Worker 값에는 Executor JVM·Python·Worker daemon·sampler가 포함된다. memory.current는 RSS가 아니며 파일 캐시를 포함한다. anon을 함께 제공한다. 공유 JVM/worker가 이전 실행의 메모리를 유지할 수 있어 전략 고유의 독립 peak로 해석하지 않는다.
- `spark-events/`, `event_metrics.csv`, `tasks.csv`, `stages.csv`, `jobs.csv`, `executor_metrics.csv`: Job/Task 수, 실패, Task 시간, 부하 비율/CV, JVM CPU, shuffle, JVM 역직렬화/결과 직렬화, GC/spill, scheduler 잔여 시간. Task 시간 합계는 wall time이 아니다. Stage별 Task max/mean·CV 및 Job/Stage 구간의 합집합도 집계한다. Executor JVM/Python process-tree RSS peak도 Event Log에서 보존하며 공유 페이지가 중복될 수 있다. JVM CPU에는 Python HDBSCAN CPU가 빠지므로 kernel CPU 및 cgroup CPU와 함께 본다.
- Spark의 `Executor Deserialize Time`과 `Result Serialization Time`은 JVM 지표다. Python pickle·Py4J·소켓 전송을 별도 순수 비용으로 완전히 분리하지 못한다. `scheduler_residual_ms`도 순수 scheduler 비용이라고 단정하지 않는다.
- `overhead_probe.csv`: 같은 선택/collect/shuffle 경로에서 HDBSCAN을 생략한 보조 측정. 반환이 기사 ID뿐이라 실제 후보 객체의 반환 비용과 다르다. kernel_sum을 wall에서 빼거나 probe 차이를 순수 병렬화 오버헤드로 단정하지 않는다.
- `environment.json`, `*.containers.json`, `images.json`, `docker-info.json`: 라이브러리·소스·Spark 설정·이미지 식별자·자원 제한.
- `SUCCESS.json`, `VALIDATED.json`: 완료 여부 및 독립 검사. 중단되거나 실패한 실험을 완료 결과로 집계하지 않는다.
- 분석 디렉터리: `summary.csv`, `all_runs.csv`, `paired_ratios.csv`, `seed_ratios.csv`, `policy_evaluation.csv`, `MEASUREMENTS.md`, PNG 그래프.

p50/p95는 실행 시간 표본의 경험 분위수(NumPy linear percentile)다. 처리량은 선택 기사 수 / p50이며 실제 스트림의 end-to-end 처리량이 아니다. speedup은 같은 Core 조건의 AS-IS 또는 CONTROL p50 / 전략 p50이다. `control_relative_efficiency`는 CONTROL 상대 speedup / p다. `strong_scaling_efficiency`는 동일 TO-BE 1 구현의 t(1)/(p*t(p))이며 고정 Driver/Master 비용이 별도 존재한다. paired ratio와 seed별 결과를 함께 제공한다. seed가 3개이므로 일반화나 통계적 유의성을 강하게 주장하지 않는다.

군집 결과는 기사 membership 집합·대표 기사 순서·centroid·confidence(10자리)를 독립 Driver 기준과 비교한다. 제외/노이즈 기사는 동일한 선택 집합의 여집합이다. 모두 빈 결과여서 우연히 통과하지 않도록 실제 군집과 제외 기사가 존재하는지도 검사한다.

첫 진단 실험 `quick-4-v1`은 결과 정합성은 통과했으나 Python parallelize 리터럴이 Task에 남는 측정 편향을 발견하여 최종 성능 집계에서 제외했다. 해당 원시 자료와 실행 코드는 감사용으로 보존한다. 첨부 `pilot_results.attachment.csv` 역시 Spark 본실험 결과에 합치지 않는다.
