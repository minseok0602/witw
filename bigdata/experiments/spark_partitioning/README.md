> 현재 실행 계약은 기본 Hash·중간 편향·심한 편향을 비교하는 **A/B 총540회·예산10,000원**입니다. [승인된 실행 계약](docs/APPROVED_RELIABILITY_RUN.md)을 확인하세요. EC2 실험은 진행 중이며,이번 커밋에는 완료된 로컬 측정만 포함했습니다. 아래720회 행렬은 앞선 설계로 보존합니다.

# Spark 연산 특성에 따른 파티셔닝 실험

운영 코드를 읽기 전용으로 사용한다. 기존 `clustering_benchmark` 코드와 결과는 보존했다. Driver/Executor 동적 전환, Core/Task 확장 실험은 이번 설계에서 제외한다. 유료 AWS 생성 전 사용자 승인이 필요하다.

현재 로컬 Quick Test 및 조건별 3회 탐색을 실행했다. **EC2 본실험 720회와 통합 C는 미실행**이다. 결과의 상세 범위·실패 기록·해석은 [로컬 검증 보고서](docs/PRE_CLOUD_REPORT.md), 단가와 시간 가정은 [비용 보고서](results/cost-analysis/COST_ESTIMATE.md)를 확인한다.

추가한 `SKEW` 조건은 테마 수 편향과 별도로 실제 Task 연산량을 의도적으로 치우치게 만든 미니 테스트다. 각 전략3회,총18회 실행을 완료했다. [설정](docs/DELIBERATE_SKEW_SETUP.md)과 [실측 결과](docs/DELIBERATE_SKEW_RESULT.md)를 확인한다. 기존 D1~D4 결과와 합쳐 분석하지 않는다.

|실험|전략|입력 및 자원|
|---|---|---|
|A 기존 이슈 매칭|A0 입력 유지 / A1 기사 수 / A2 후보 수 비용|4,000건, 768차원, 초기 4파티션|
|B 신규 이슈 군집화|B0 테마 hash / B1 기사 수 LPT / B2 실측 비용 LPT|2,000건, 테마 전체를 한 Task에 배치|
|B 별도 기준|AS_IS 테마별 collect / CONTROL 일괄 collect|동일 입력·원본 HDBSCAN|

두 Worker, Executor당 2 Core, 총 4 Core와 목표 4파티션으로 고정한다. A 360회 + B 주요군 360회 = 720회다. B의 두 Driver 기준군 240회, 워밍업, 보정, Quick 및 C는 별도다. 탐색 결과로 승자를 선정하지 않는다.

## 파일

- [설계 및 측정 계약](docs/DESIGN.md), [운영 코드 분석](docs/CODE_ANALYSIS.md)
- `apps/data.py`: 고정 스냅샷·입력 생성, `placement.py`: 분산 목적지·LPT 계획
- `apps/matching.py`, `clustering.py`: 원본 매칭·군집화 함수를 사용하는 비교군
- `apps/benchmark.py`, `cost_model.py`: 무작위 교차 반복·독립 Executor 비용 모델 보정
- `apps/validate.py`, `event_metrics.py`, `resource_monitor.py`: 결과·할당·Event Log·자원 검증
- `apps/analyze.py`, `forecast.py`: paired 통계·그래프·시간 및 비용 추정
- [EC2 실행 및 회수](infra/README.md), `integration/stream_adapter.py`, `integration/plan.py`

## 로컬 재현

저장소 루트의 PowerShell, Docker Desktop Linux engine, 호스트 Python 3이 필요하다. 측정용 Spark/Python은 Docker 안에서 실행한다. Docker에 최소 약 7 Core/12 GiB를 제공하고 실행 중 PC 절전·Docker 중단을 피한다. 외부 운영 서비스는 기동하지 않는다.

```powershell
# 기존 프로젝트 이미지가 없다면 먼저 빌드
docker build -t news-spark:4.2.0-kafka bigdata/spark
# Compose가 참조하는 실험 이미지 태그
docker build -t witw-clustering-lab:local bigdata/experiments/spark_partitioning

& bigdata/experiments/spark_partitioning/run_local.ps1 -Application A -Mode quick -RunTag myquick
& bigdata/experiments/spark_partitioning/run_local.ps1 -Application B -Mode quick -RunTag myquick
& bigdata/experiments/spark_partitioning/run_local.ps1 -Application A -Mode explore -Repetitions 3 -RunTag myexplore
& bigdata/experiments/spark_partitioning/run_local.ps1 -Application B -Mode explore -Repetitions 3 -IncludeBaselines -CostModelRun B-quick-myquick -RunTag myexplore
```

같은 RunTag 결과를 덮어쓰지 않는다. B Quick은 144개 별도 보정 표본으로 모델을 만들고 탐색은 이 모델을 동결해 재사용한다. 모든 실행은 고유 `results/<App>-<mode>-<tag>`에 원시 결과·실행 코드 사본·환경·Event Log를 저장한다. 정상 종료 여부만으로 충분하지 않으며 `VALIDATED.json.passed=true`를 확인한다. 중단된 실행은 원본 그대로 보존한다.

호스트의 분석용 의존성은 측정 컨테이너와 별도다.

```powershell
python -m pip install -r bigdata/experiments/spark_partitioning/requirements-analysis.txt
python bigdata/experiments/spark_partitioning/apps/analyze.py bigdata/experiments/spark_partitioning/results/A-explore-myexplore bigdata/experiments/spark_partitioning/results/B-explore-myexplore --output bigdata/experiments/spark_partitioning/results/my-analysis --plots
```

`forecast.py`는 제공된 최종 로컬 탐색 디렉터리를 대상으로 비용 보고서를 재생성한다. 새 실행을 비용 근거로 사용할 경우 이 파일의 입력 실행 ID를 명시적으로 바꾼다. Quick/explore/full은 한 분석에 혼합하지 않는다.

## 결과 읽기

`runs.csv`의 측정 구간은 준비된 입력부터 전략의 추가 집계·계획·Shuffle·원본 연산·최종 collect까지다. 공통 입력 준비·원본 참조 검증·결과 CSV 저장은 제외하며 `common_costs.csv`와 전체 컨테이너 시간에 별도 기록한다. 워밍업 행은 원시에 남고 통계에서는 제외한다.

`partition_runs.csv`는 실제 TaskContext 목적지·기사 수·테마·예상 비용·Python kernel CPU/시간/RSS, `tasks.csv`·`stages.csv`·`jobs.csv`는 Event Log 원시 계측이다. `resources-*.csv`는 250ms cgroup 샘플이다. `enriched_runs.csv`, 분석 `summary.csv`·`paired.csv`는 이 원시 자료에서 재생성할 수 있다. JVM 직렬화 지표는 Python 직렬화 전체를 의미하지 않는다. 병렬 Task 시간을 wall에서 빼서 오버헤드를 계산하지 않는다.

`source_audit.json`은 분석 시작 시점의 운영 파일 SHA256이다. 기존 사용자 변경분을 포함한 현재 작업 트리 기준이며 원격 커밋과 같다는 뜻은 아니다. 실행 시 이 값과 다르면 조용히 다른 연산을 측정하지 않고 중단한다.

## 통합 C의 현재 상태

원본 stream 함수의 연산 배치만 교체하는 실험용 AST adapter와 LLM 고정 응답, C0~C3 계획 생성기를 작성하고 정적 계약 테스트를 수행했다. **실제 Kafka·DB Writer·PostgreSQL·HDFS를 기동하는 C 배포 및 E2E fixture/측정 실행기는 아직 완성하지 않았다.** A/B 본실험으로 전략을 검토한 후 구현·검증해야 한다. C에서 각 app의 cores.max=2, executor.cores=1로 전체 4 Core를 공유한다. A/B 독립 결과로 C의 처리량·정합성을 대신 주장하지 않는다.

소스 감사는 Git의 Windows/Linux 줄바꿈 차이만 정규화해 검사한다. 측정 당시 원래 바이트 SHA는 `docs/MEASURED_SOURCE_AUDIT.json`과 실행별 environment.json에 보존했다. 이식성을 위한 감사/실행 준비 코드 보완은 수치 연산 함수를 변경하지 않는다.

검증 로그 사본은 [results/verification](results/verification/)에 있다. 원래 로컬 작업 폴더의 진행 중 제어기는 이 Git 병합 작업으로 변경하거나 재시작하지 않았다.
