# 파이프라인 전달 보장과 장애 관측

## 전달과 중복 처리

Collector는 본문 수집·검증 후 SQLite `kafka_outbox`에 원본 이벤트 전체를 저장한다.
중복 판정·수집 한도 예약과 outbox 삽입, 발견 대기열 제거는 하나의 트랜잭션이다.
Kafka ACK 콜백이 성공한 경우에만 outbox에서 제거한다. 큐 등록 성공은 전송 완료가 아니다.
실패하거나 ACK 전에 종료되면 다음 실행/수집 라운드에서 본문 재수집 없이 전송한다.
outbox는 발견 대기열의 보관 기간이나 매체 정리 대상에 포함되지 않는다.
`kafka_failed.jsonl`은 진단 기록이며 재전송의 기준은 SQLite outbox다.

Spark가 발행하는 DB 이벤트에는 `event_id`가 추가된다.

| 이벤트 | 식별 기준 |
|---|---|
| ARTICLE_UPSERT | 이벤트 종류와 정규화된 payload의 SHA-256. 추론 실행 시각 변경은 제외하며 수정된 payload는 새 이벤트다. |
| ARTICLE_ISSUE_ASSIGNED | 이벤트 종류와 정규화된 payload의 SHA-256 |
| ISSUE_CREATE | 테마와 정렬된 기사 ID 집합. 재시도에서 LLM 이름이나 실행 시각이 바뀌어도 동일하다. |

Backend는 `pipeline_event_inbox`에 이벤트를 기록하고 도메인 변경과 APPLIED 전환을
같은 DB 트랜잭션에서 커밋한다. 이미 APPLIED인 ID는 다시 반영하지 않는다.
처리 완료 후 payload는 비우고 중복 방지에 필요한 ID·종류·시각은 유지한다.
기존 producer의 ID 없는 이벤트도 수신하며, 종류와 payload에서 대체 ID를 만든다.
기존 메시지의 payload 직렬화 순서가 바뀌는 경우까지 대체 ID의 동일성을 보장하지는 않는다.

기사/이슈가 아직 저장되지 않았다면 해당 이벤트를 PENDING으로 보관하고 Kafka 소비는 계속한다.
이 경우에는 도메인 쓰기를 시작하지 않는다. 운영 Backend의 스케줄러가 5초 간격으로
최대 100건을 다시 확인한다. 마지막 시도 순으로 순환하고 행 잠금으로 중복 반영을 방지한다.
스케줄러가 DB 오류를 만나면 payload와 PENDING 상태를 유지한다.
형식 오류나 영구적인 참조 테마/국가 오류는 기존 Kafka 재시도·DLT 정책을 따른다.

전달 방식은 at-least-once다. ACK 직후 로컬 완료 처리 전에 종료되면 재전송할 수 있지만
최종 DB의 동일 이벤트 반영은 억제한다. HDFS/원본 JSONL에는 물리적 중복이 남을 수 있다.
inbox ID를 삭제하면 해당 구간의 중복 방지 보장도 사라지므로 임의로 정리하지 않는다.

## 수집과 경보

| 관측 대상 | 지표·판정 |
|---|---|
| Collector | `:9103/metrics`: outbox 건수·최장 대기 시간, ACK·실패 누계, 마지막 라운드 완료 시각 |
| DB Writer | `/actuator/prometheus`: applied/duplicate/deferred/failed 처리 누계, PENDING 건수·최장 대기 시간·관측 갱신 시각 |
| 수집 → DB 지연 | 기사 `crawled_at`(수집 발견 시각)부터 DB 커밋 완료까지의 시간 히스토그램 |
| Kafka | DB Writer 그룹 연결·lag·커밋 정지, `news.event.v1.dlt` 보관 건수 |
| 알림 전달 | Alertmanager 가용성과 webhook 전송 실패 |

주요 기준은 Collector 전송 대기 15분, DB 참조 대기 5분,
DB Writer 커밋 진전 없음 5분, 수집→DB p95 지연 15분이다.
규칙의 `for` 시간과 세부 조건은 `bigdata/monitoring/prometheus/alerts.yml`을 따른다.
기본 DB Writer 그룹 이름은 `db-writer-*`이며 다른 접두사를 사용하면 exporter 필터와 규칙도 함께 변경한다.

DLT 경보는 보관된 레코드의 존재를 뜻한다. 이미 수동 복구했는지를 자동으로 판정하지 않는다.
원인 해결과 DB 반영 확인 후 운영자가 경보를 관리한다. 이 변경에는 DLT 자동 재발행을 포함하지 않는다.

## 환경 설정과 반영 순서

1. Backend를 먼저 반영해 Flyway V20과 inbox 처리를 적용한다.
2. Backend 운영 환경의 `BACKEND_METRICS_BIND_HOST`를 Backend 사설 인터페이스 IP로 지정한다.
   기본값은 `127.0.0.1`이다. BigData 호스트에서 해당 사설 IP의 9091 포트에 접근할 수 있어야 한다.
3. BigData 환경 파일의 `BACKEND_METRICS_HOST`에 같은 사설 IP를 지정한다.
4. Collector와 Spark를 반영한다. 기존 Collector SQLite 볼륨은 그대로 사용하며 `--reset`하지 않는다.
5. BigData 환경 파일의 `ALERT_WEBHOOK_URL`에 수신 URL을 지정하고 모니터링을 반영한다.

`ALERT_WEBHOOK_URL` 수신기는 **Alertmanager webhook JSON 형식**을 지원해야 한다.
Slack/Mattermost 전용 incoming webhook에는 해당 서비스용 수신기나 변환 어댑터가 필요하다.
주소가 비어 있으면 외부 전송 없이 Prometheus/Alertmanager에서 경보 상태를 확인한다.
설정 생성기는 URL을 JSON/YAML 문자열로 인코딩하고 구성 파일을 제한된 권한으로 보관한다.
CI 배포는 설정을 검증한 뒤 Prometheus와 Alertmanager를 재로드한다.
실제 외부 수신 성공 여부는 수신 주소 연결 후 확인해야 한다.

## Grafana에서 장애와 알림 확인

`WITW 장애 감지와 전달 상태` (`/d/witw-pipeline-reliability`) 대시보드는 지표 수집 상태,
pending/firing 경보, Collector outbox, DB 참조 대기, Kafka lag, 수집→DB p95 지연,
웹훅 전송 시도·실패를 함께 보여준다. 처리 표본이 없는 지연 지표는 `No data`로 표시한다.

전달 경로는 `애플리케이션 지표 → Prometheus 규칙 → Alertmanager → ALERT_WEBHOOK_URL`이다.
Grafana에는 Prometheus 경보 조회와 외부 Alertmanager 데이터 소스를 자동 등록한다.
`Alerting → Alert rules`에서 data source-managed 규칙을 확인하고,
`Alerting → Silences`에서 `Pipeline Alertmanager`를 선택해 정기 작업의 알림을 일시 중지할 수 있다.
이 Alertmanager 연결은 BigData 모니터링 스택의 Grafana에 등록된다. 별도의 infra Grafana에서는
`Prometheus-BigData` 데이터 소스를 통해 같은 경보 규칙과 대시보드를 조회한다.
Prometheus Alertmanager의 수신 주소·전송 정책은 저장소 설정으로 관리한다.
Grafana에 같은 경보 규칙과 contact point를 중복 생성할 필요는 없다.

환경 파일의 `ALERT_WEBHOOK_URL`을 변경했다면 설정 생성기를 다시 실행해야 한다.
아래 명령은 저장소 루트에서 실행하며 `/opt/news-bigdata/.env`는 실제 배포 환경 파일로 바꾼다.

```bash
docker compose --env-file /opt/news-bigdata/.env -f bigdata/monitoring/docker-compose.yml run --rm --no-deps alertmanager-config
docker compose --env-file /opt/news-bigdata/.env -f bigdata/monitoring/docker-compose.yml up -d --no-deps alertmanager grafana
docker compose --env-file /opt/news-bigdata/.env -f bigdata/monitoring/docker-compose.yml kill -s HUP alertmanager
docker compose --env-file /opt/news-bigdata/.env -f bigdata/monitoring/docker-compose.yml restart grafana
```

마지막 재시작은 기존 Grafana에 새 데이터 소스 provisioning을 반영한다. CI 배포에도 포함한다.

검증은 실제 장애에서 `pending → firing → resolved`를 확인하고 수신 서버의 HTTP 기록과 대조한다.
예를 들어 Collector 지표 수집 중단은 2분 유지 후 경보가 발생하고, outbox 대기는
15분 초과 상태가 2분 유지된 후 발생한다. 여기에 15초 수집·평가 주기와
Alertmanager 최초 그룹 대기 30초, 이후 그룹 갱신 5분이 더해질 수 있다.
복구 알림은 `send_resolved: true`로 전송한다.

웹훅 실패 지표는 수신 서비스가 성공 응답한 뒤 사람에게 표시했는지까지 보장하지 않는다.
또한 Prometheus/Alertmanager가 함께 중단되면 자체 알림도 중단되므로,
운영에서는 별도 호스트의 외부 생존 감시를 추가해야 이 경우까지 감지할 수 있다.
실험용 로컬 HTTP 수신 성공과 실제 운영 채널의 수신 성공은 구분해 기록한다.

참고: [Grafana Prometheus 경보](https://grafana.com/docs/grafana/latest/datasources/prometheus/alerting/),
[Grafana Alertmanager 연결](https://grafana.com/docs/grafana/latest/datasources/alertmanager/),
[Alertmanager 웹훅 설정](https://prometheus.io/docs/alerting/latest/configuration/#webhook_config).

## 확인 방법

```bash
python -m unittest discover -s bigdata/collector/tests
python -m unittest discover -s bigdata/monitoring/tests
# monitoring Compose는 LOKI_URL 등 기존 필수 환경변수가 필요하다.
docker compose -f bigdata/monitoring/docker-compose.yml run --rm --no-deps \
  --entrypoint promtool prometheus test rules /etc/prometheus/alert_rules.test.yml
cd backend
./gradlew test --tests '*infra.kafka.*'
```

Backend 통합 테스트는 프로젝트의 `TestContainerSupport`를 사용한다.
CI에서는 `TEST_POSTGRESQL_*`, `TEST_VALKEY_*`로 전달하는 격리된 테스트 DB를 사용한다.
순서 역전 후 반영, 중복 전달, 도메인 실패 시 inbox 롤백을 검증한다.

운영 진단에서는 다음 조회로 대기 중인 이벤트부터 확인한다.

```sql
SELECT event_id, event_type, received_at, last_attempt_at
FROM pipeline_event_inbox
WHERE status = 'PENDING'
ORDER BY received_at;
```

노드 복제·백업, NLP 처리 시간 제한, 모델 실패 점수 처리, 인증 정책은 이번 변경 범위에 포함하지 않는다.
