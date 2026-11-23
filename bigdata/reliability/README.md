# 실제 구성요소를 사용하는 파이프라인 장애 실험

운영 코드의 전달 보장과 경보를 검증하는 **로컬 격리 실험**이다. Kafka ACK, PostgreSQL 트랜잭션,
Spark micro-batch, Kafka 소비 offset, Prometheus 평가, Alertmanager HTTP 전송을 실제 실행한다.
외부 웹 수집과 GPU/LLM 추론은 고정 입력으로 대체한다. 운영 성능이나 가용성 SLA를 측정하는 도구는 아니다.

## 구성과 대조군

- Compose project: `witw-reliability-live`. 업무 서비스는 `internal: true` 네트워크에 둔다.
- 호스트 제어 포트는 localhost에만 연다. 실제 사용자 채널 대신 로컬 HTTP 수신기에 알림을 보낸다.
- 동일 Kafka 입력을 기존/개선 Backend가 각각 다른 consumer group과 DB로 소비한다.
- 기존 Collector 및 Backend의 Consumer/Service 두 파일은 `git show <baseline>:<path>`로 복원한다.
  나머지 공통 클래스, optional event ID, V20 스키마는 공통이다. 기존 Consumer는 inbox를 사용하지 않는다.
  따라서 과거 배포 이미지 전체를 재현한 실험과는 구분한다.
- Collector에는 동일 생성 규칙·개수의 별도 run namespace 입력을 제공하고 실제 예약·중복 판정·SQLite·producer를 호출한다.
  두 Collector의 URL/ID 자체는 실험 간 오염을 막기 위해 다르다. Backend 대조군에는 정확히 같은 Kafka 레코드가 전달된다.
  현재 outbox는 Linux Docker named volume에 보관한다. 장시간 경보 실험의 SQLite와 Spark checkpoint는
  Windows bind mount이며 HDFS가 아니다.
- NLP는 운영 consumer/producer/archive 함수를 사용한다. 추론 결과만 결정적 768차원 밀집 벡터로 고정한다.
- 실제 enriched stream을 `local[2]`, driver 1 GB로 실행한다. trigger 15초, Collector poll 600초와
  운영 경보 임계값을 유지한다. 업무 부하를 생성한 뒤 완료를 기다리는 burst 실험이다.

## 실행

기존 실험의 볼륨/마커가 있는 상태에서 초기화 명령을 다시 실행하지 않는다. 새 측정은 별도 project,
포트, state root를 사용하거나 기존 증거를 보존하고 명시적으로 실험 환경을 초기화해야 한다.
운영 `.env`나 실제 webhook URL을 이 디렉터리에 복사할 필요가 없다.

Windows PowerShell, Docker Desktop Linux engine, Python 3, JDK 21/Gradle 9.1 build image가 필요하다.
실제 측정에 사용한 이미지 ID와 호스트 자원은 결과의 `environment.json`에 저장한다.
측정 당시 다음 로컬 이미지를 사용했다. 새 환경에서는 먼저 해당 이미지를 준비해야 한다.

```powershell
# Collector와 Spark는 저장소 Dockerfile로 빌드 가능하다.
docker build -t codex-pipeline-collector:verify bigdata/collector
docker build -t news-spark:4.2.0-kafka bigdata/spark
docker build -f bigdata/reliability/Spark.Dockerfile -t witw-reliability-spark:verify bigdata/reliability
# news-kafka-hdfs-writer-test:latest는 JDK 21 + Gradle 9.1을 제공하는 로컬 build/debug image다.
# 깨끗한 환경에서는 LAB_BUILD_IMAGE 환경변수에 Gradle 9.1/JDK 21 이미지를 지정한다.
python -m pip install --target bigdata/reliability/.generated/python-deps confluent-kafka psycopg[binary] requests
$env:LAB_BASELINE_REV = '9744533d2056f92fedca6798267d6a04aa5ab08f'
python bigdata/reliability/prepare.py --build
docker compose -p witw-reliability-live -f bigdata/reliability/compose.yml up -d kafka postgres redis toxiproxy receiver prometheus alertmanager
python bigdata/reliability/init_lab.py
docker compose -p witw-reliability-live -f bigdata/reliability/compose.yml up -d backend-before backend-after
```

다음 관측기/실험은 **별도 터미널에서 동시에** 실행한다. 첫 번째 경보 실험은 운영 임계값 때문에 약 20분이 필요하다.
그동안 반복 장애 실험을 진행할 수 있다.

```powershell
python bigdata/reliability/observe_notifications.py
python bigdata/reliability/watch_long.py
python bigdata/reliability/run_experiments.py --repetitions 3 --count 1000
```

기본 시나리오는 중복 이슈, 기사보다 먼저 도착한 이슈, DB 커밋 직전/직후 강제 종료,
40초 DB 응답 정지, Collector 예약 커밋 직후/ACK 직후 강제 종료다.
입력 body는 약 0.9–6.5 KB, 임베딩은 기사마다 다른 768차원 값이다.
각 round의 Collector 대조군 순서를 교대한다. 반복 횟수는 장애 회복 재현성을 확인하는 수준이다.

```powershell
# 보충 Collector 실험만 실행할 때
python bigdata/reliability/run_experiments.py --collector-only --start-repetition 4 --repetitions 1 --count 1000
# 별도 burst 관측(이전 장애 실험 종료 후)
python bigdata/reliability/load_soak.py 10000
# 완료한 run별 독립 검증 및 증거 수집; 실제 출력 경로로 바꾼다.
python bigdata/reliability/wait_pipeline_idle.py bigdata/reliability/results/<run>
python bigdata/reliability/verify_evidence.py bigdata/reliability/results/<run>
python bigdata/reliability/capture_evidence.py bigdata/reliability/results/<run>
docker compose -p witw-reliability-live -f bigdata/reliability/compose.yml stop
```

관측기 종료는 `.generated/state/stop-notification-observer` 파일을 생성한다.
수집기의 fixture entrypoint는 운영 heartbeat 파일을 만들지 않는다. 이미지에서 상속된 healthcheck의
`unhealthy` 표시는 이 fixture의 준비 상태 판정에 사용하지 않으며, 마커·실제 metrics·Kafka offset·DB로 확인한다.

## 측정 규약과 증거

1. 입력 ID/내용의 oracle을 전송 전에 파일로 고정한다. body SHA-256과 전체 벡터를 DB/Kafka와 대조한다.
2. 독립 Kafka consumer가 topic/partition/offset/event ID/hash를 JSONL에 기록한다.
3. DB audit trigger가 **커밋된** 이슈 INSERT/UPDATE 횟수를 기록한다. 두 버전에 같은 trigger를 설치한다.
4. DB 장애 경계는 시험용 JDWP/JDI로 정확한 event ID의 메서드 종료에서 JVM 전체를 정지한다.
   kill 전 DB 가시성과 Kafka 미커밋 offset을 확인하고 외부 `docker kill --signal=KILL`로 종료한다.
   재시작 후 이미 보이는 DB 값만 보고 복구를 선언하지 않고 해당 Kafka offset의 실제 커밋도 기다린다.
5. Collector trace는 해당 운영 메서드에서 무한 대기한 뒤 컨트롤러가 SIGKILL한다.
   `pause_protocol=blocking-trace-v2` 마커가 있는 결과만 정식 Collector 집계에 포함한다.
   PID namespace의 PID 1이 자기 자신에게 보낸 SIGSTOP을 무시한 초기 실험은 제외한다.
6. 장시간 네트워크 장애는 Toxiproxy의 실제 TCP 연결을 끊는다. host monotonic clock으로 장애 주입부터
   로컬 HTTP 알림 최초 관측까지, 연결 복구부터 outbox ACK 완료까지를 각각 측정한다.
7. 모든 지연은 이 로컬 자원 배치와 fixture의 관측값이다. 공유 호스트에서 두 Backend를 함께 실행하므로
   버전 간 속도 차이를 개선 효과로 해석하지 않는다. burst 완료율을 지속 처리량으로 표현하지 않는다.

`summary.json`, `verification.json`, source/image hash와 실험 보고서는 작은 결과물이다.
대용량 원본 `results/*/raw/`와 `.generated/`는 Git에서 제외한다. 원본은 로컬에 보존하며
`evidence-sha256.json`으로 무결성을 대조할 수 있다. 초기 protocol pilot도 지우지 않는다.
다중 노드 Kafka failover, HDFS 복구, 호스트/볼륨 유실, 실제 모델 품질·GPU 처리량은 검증 범위 밖이다.

별도 burst는 누적된 단일 fault theme와 구분한 20개 theme × 20개 활성 이슈 후보를 사용한다.
theme마다 정답 중심 벡터 1개와 교란 후보 19개를 고정하며, 실제 Spark의 theme 필터와 cosine matching을 실행한다.
10,000기사의 벡터 차원 비교 규모는 `10,000 × 20 × 768 = 153,600,000`이다.
이 catalog 크기를 넘어선 운영 용량을 주장하지 않는다. burst 요약에는 본문 hash와 전체 벡터 대조도 포함한다.
