"""Render a Korean STAR report using measured, qualified results only."""
import argparse
import json
from pathlib import Path

from run_experiments import LAB, ROOT


def read(path):
    return json.loads(path.read_text(encoding="utf-8"))


def write_report(primary, supplement, soak):
    aggregate = read(primary / "aggregate.json")
    groups = aggregate["groups"]
    duplicate = groups["duplicate_events"]
    reordered = groups["reordered_dependencies"]
    pre = groups["db_precommit_sigkill"]
    post = groups["db_postcommit_sigkill"]
    outage = groups["database_unresponsive_40s"]
    long = read(primary / "raw/long-network/observation.json")
    lifecycle = read(primary / "raw/long-network/notification-lifecycle.json")
    environment = read(primary / "environment.json")
    source = read(primary / "source-manifest.json")
    unique_inputs = sum(item.get("input_articles", 0) for item in groups.values()) + pre["trials"] + post["trials"] + long["count"]
    summary = read(soak / "summary.json") if soak else None
    db = duplicate["versions"]
    avoided = db["before"]["committed_issue_writes"] - db["after"]["committed_issue_writes"]
    write_reduction = 100 * avoided / db["before"]["committed_issue_writes"]
    rows = []
    for point, label in (("outbox_commit", "예약 커밋 직후"), ("ack_before_delete", "ACK 콜백 / outbox 삭제 전")):
        values = [groups[f"collector_{point}_sigkill:{version}"] for version in ("before", "after")]
        old, new = values
        rows.append(f"| {label} | {old['trials']} / {new['trials']} | {old['unique_kafka_articles']:,}/{old['input_articles']:,} | {new['unique_kafka_articles']:,}/{new['input_articles']:,} | {new['kafka_records'] - new['unique_kafka_articles']:,} |")
    cpu = environment["host_hardware"]["cpu"]
    cpu = cpu[0] if isinstance(cpu, list) else cpu
    hardware = environment["host_hardware"]
    docker = environment["docker"]
    body = f"""# 파이프라인 장애 복구 실험 — 실제 구성요소, 고정 입력

실험일: 2026-11-23 (KST). **실제 로컬 격리 스택에서 측정했다.** 외부 수집과 모델 추론은 고정 입력이며,
운영 환경의 SLA·처리 용량을 입증한 실험은 아니다. 정식 집계는 **{aggregate['qualified_trials']}개 장애/중복 시나리오**와
별도 장시간 네트워크 장애 1회다. 아래 숫자는 결과 JSON에서 생성했다.
이 범위의 고유 입력 기사는 **{unique_inputs:,}건**이며, 같은 입력을 두 Backend가 소비한 것을 두 배로 세지 않았다.

## S — 상황

수집 예약은 끝났으나 Kafka 전송 전인 구간, Kafka ACK 이후 로컬 완료 처리 전인 구간,
DB 커밋과 Kafka offset 커밋 사이의 구간에서 프로세스가 죽을 수 있다.
기사와 이슈가 다른 partition에서 순서가 뒤바뀌어 도착하거나 DB가 응답하지 않는 상황도 재현했다.
앞서 수행한 mock/단위 테스트 결과와 이 실제 스택 관측을 구분한다.

## T — 검증 목표

- 입력 명세의 기사 ID가 최종 DB에 도달하며 제목·요약·테마·768차원 벡터와 이슈 연결이 맞는가.
- 동일 이벤트 재전송 시 커밋된 도메인 쓰기가 반복되는가.
- 프로세스 재시작 후 실제 Kafka replay와 offset 커밋까지 완료되는가.
- 운영 경보 임계값을 유지했을 때 실제 HTTP 웹훅이 도착하는가.

## A — 실행 환경과 방법

| 항목 | 실제 실험 구성 |
|---|---|
| 호스트 CPU | {cpu['Name']}; {cpu['NumberOfCores']} cores / {cpu['NumberOfLogicalProcessors']} logical processors |
| 호스트 RAM | {hardware['ram_bytes'] / 2**30:.2f} GiB |
| Docker VM | {docker['NCPU']} logical CPUs, {docker['MemTotal'] / 2**30:.2f} GiB; {docker['OperatingSystem']} |
| Kafka | apache/kafka:4.3.1; 단일 broker/controller, RF=1; raw/enriched 6 partitions, event/DLT 3 |
| PostgreSQL / Redis | PostgreSQL 17, Redis 7; 기존/개선 DB와 Kafka group 분리 |
| Backend | JDK 21; 버전별 CPU 2 / 메모리 1.5 GiB, JVM Xmx 768 MiB |
| Spark | 실제 production enriched stream, local[2], CPU 2 / 메모리 3 GiB, driver 1 GiB, 15초 trigger |
| Collector | 실제 SQLite 예약/producer/outbox, CPU 1 / 메모리 512 MiB; poll 600초 |
| 관측 | Prometheus 15초 scrape/evaluation, 실제 운영 alerts.yml, Alertmanager → 로컬 HTTP 수신기 |

기존 코드 기준 commit은 `{source['baseline_commit']}`다. Collector와 Backend Consumer/Service를 해당 commit에서
복원한 대조군이며, V20 스키마와 다른 공통 클래스는 공유한다. 과거 배포 이미지 전체의 복제는 아니다.
Backend 두 버전은 같은 Kafka 레코드를 각각 소비한다. Collector는 같은 생성 규칙의 별도 run ID를 사용한다.
입력은 1,000기사 단위, 약 0.9–6.5 KB 본문, 기사마다 다른 정규화 768차원 밀집 벡터다.
벡터 원소만 float64로 계산하면 `1,000 × 768 × 8 = 6,144,000 bytes ≈ 5.86 MiB`이고,
이는 JSON·Python/JVM 객체와 처리 중 복사본의 메모리를 포함하지 않는다.

DB 장애 경계는 정확한 event ID를 검사하는 JDI MethodExit에서 JVM 전체를 정지했다.
DB 가시성과 target Kafka offset의 미커밋을 확인한 뒤 외부 SIGKILL을 보냈다.
재시작 후 해당 offset이 실제 커밋될 때까지 기다렸다. Collector는 trace frame에서 실행을 막고 SIGKILL했다.
DB 응답 정지는 실제 PostgreSQL 컨테이너 pause/unpause, 네트워크 장애는 실제 TCP proxy 차단이다.

반복 장애 실험은 전송 전에 입력 명세를 고정했다. 독립 Kafka consumer가 레코드 ID·offset·내용 hash를 기록했다.
별도 장시간 100건 실험은 명세 고정 기능을 추가하기 전에 시작하여, 기대 ID/내용을 알려진 생성 규칙과 건수에서
사후 재구성하고 실제 Kafka hash와 DB로 대조했다. 이 차이는 `manifest-provenance.json`에 기록했다.
독립 SQL 검증은 입력 ID와 추가 ID, 제목/요약/테마/URL/국가, 전체 벡터 원소(허용오차 1e-6), 이슈 연결을 대조한다.
Backend의 정상 요약 trim을 기대 결과에 반영한다. 같은 DB audit trigger로 **커밋된** 이슈 쓰기를 집계했다.
최종 관측에는 NLP group offset, Spark checkpoint commit, 두 Backend group offset과 PENDING inbox도 확인한다.

## R — 실제 관측 결과

| 시나리오 | 반복 / 입력 | 기존 | 개선 |
|---|---|---|---|
| 중복 이슈 20% 재전송 | {duplicate['trials']}회, 기사 {duplicate['input_articles']:,}건 | 이슈 쓰기 {db['before']['committed_issue_writes']:,}회 | {db['after']['committed_issue_writes']:,}회 |
| 이슈를 기사보다 10초 먼저 전송 | {reordered['trials']}회, 기사 {reordered['input_articles']:,}건 | 미연결/오연결 {reordered['versions']['before']['incorrect_assignments']:,}건 | {reordered['versions']['after']['incorrect_assignments']:,}건 |
| DB 커밋 전 SIGKILL | {pre['trials']}회 | 이슈 쓰기 {pre['committed_issue_writes']['before']}회 | {pre['committed_issue_writes']['after']}회 |
| DB 커밋 후, Kafka ACK 전 SIGKILL | {post['trials']}회 | 이슈 쓰기 {post['committed_issue_writes']['before']}회 | {post['committed_issue_writes']['after']}회 |
| 약 40초 DB 무응답 | {outage['trials']}회, 기사 {outage['input_articles']:,}건 | 누락 {outage['versions']['before']['missing_ids']}건 | 누락 {outage['versions']['after']['missing_ids']}건 |

중복 재전송에 의한 불필요한 이슈 쓰기 **{avoided}회**를 억제했고 시험 대상 이슈 쓰기는 **{write_reduction:.2f}%** 줄었다.
이 수치는 시험에서 발생시킨 중복 비율에 대한 값이며 운영 트래픽의 감소율이 아니다.
순서 역전의 DLT 관측은 총 {reordered['dlt_records']}건이었다. 기존 버전도 기사 자체는 저장하므로,
기사 건수만 비교했으면 이슈 연결 실패를 놓쳤을 것이다.

| Collector 종료 지점 | 기존/개선 유효 반복 | 기존 Kafka 고유 기사 / 입력 | 개선 Kafka 고유 기사 / 입력 | 개선 물리적 재전송 중복 |
|---|---|---|---|---|
{chr(10).join(rows)}

ACK 후 종료에서는 물리적 Kafka 중복이 허용된다. 보장 대상은 durable outbox의 재시도와 최종 DB의 이벤트별 중복 반영 억제다.
각 trial의 최종 DB 누락/내용/이슈 연결 결과는 전체 pipeline drain 후 `verification.json`에 ID 목록까지 남긴다.
`summary.json`의 최초 trial 스냅샷은 원본 그대로 보존한다. 초기 runner의 180초 관측 제한이 지나도
Spark가 처리 중일 수 있으므로 이 스냅샷의 미연결 건수를 영구 누락으로 판정하지 않는다.
집계는 전체 단계의 offset/checkpoint drain 후 독립 검증값을 사용하며, 보충 runner는 관측 제한을 600초로 늘렸다.
Collector 기존 버전의 상류 누락은 어느 Backend를 사용해도 복구할 수 없으므로 Backend 비교 효과와 분리한다.

DB 무응답의 실제 pause 구간은 {outage['fault_duration_seconds']['min']:.2f}–{outage['fault_duration_seconds']['max']:.2f}초,
응답 복구부터 두 consumer drain 확인까지는 중앙값 {outage['restore_to_drained_seconds']['median']:.2f}초
(범위 {outage['restore_to_drained_seconds']['min']:.2f}–{outage['restore_to_drained_seconds']['max']:.2f}초)였다.
JVM 재시작에는 Spring 초기화와 group 재가입이 포함된다. 속도 차이를 개선 효과로 해석하지 않는다.

### 운영 임계값을 유지한 실제 알림

기사 {long['count']}건을 outbox에 남기고 TCP 연결을 끊었다. `CollectorDeliveryStalled` 웹훅은 장애 주입 후
**{long['fault_to_notification_observed_seconds']:.3f}초 ({int(long['fault_to_notification_observed_seconds'] // 60)}분 {long['fault_to_notification_observed_seconds'] % 60:.3f}초)**에
로컬 수신기에서 관측됐다. 연결 복구 후 outbox ACK 완료는 **{long['restore_to_outbox_drained_seconds']:.3f}초**였다.
별도 0.5초 관측기에서도 firing을 {lifecycle['fault_to_first_firing_observed_seconds']:.3f}초에 확인했고,
outbox drain 관측 후 resolved 웹훅은 {lifecycle['outbox_drain_to_resolved_observed_seconds']:.3f}초에 확인했다.
전용 복구 watcher와 별도 알림 관측기의 polling 시점 차이를 그대로 보존했다.
이는 외부 사용자 채널의 수신 확인이 아니다. 운영 규칙은 age 900초 초과 + `for: 2m`, Alertmanager group wait 30초다.
따라서 현재 설정은 수분 이내의 장애 통지를 목표로 하는 경우 조정이 필요하다.
재전송도 600초 수집 주기를 따르므로, 빠른 복구가 필요하면 재전송 주기를 수집 주기에서 분리하는 개선이 후보가 된다.

### 측정 도구 검증과 제외 기준

초기 두 pilot은 replay offset 관측과 event ID 경계 검사를 보완하는 데 사용했고 정식 집계에서 제외했다.
정식 run 초기 Collector 4건도 namespace PID 1의 self-SIGSTOP 무시를 독립 재현하여 제외했다.
그 원본을 삭제하지 않았고 `protocol-status.json`에 이유를 기록했다.
이후 `pause_protocol=blocking-trace-v2` 결과만 Collector 집계에 포함하고 부족한 반복을 보충했다.
보충 CLI의 인자 연결 오류로 입력 전 종료한 실행도 별도 aborted 상태로 보존했다.
제품 코드 자체는 정식 측정 도중 수정하지 않았다.

## 해석의 한계

- 외부 기사 수집, 번역·요약·임베딩 모델/GPU, LLM issue 생성, 별도 clustering job은 실행하지 않았다.
  기존 enriched stream의 실제 issue matching은 실행했다. 모델 품질이나 NLP 장애 복구를 입증하지 않는다.
- Spark는 local[2]이며 checkpoint는 file bind mount다. HDFS/분산 worker 복구, broker failover,
  물리 호스트·볼륨 손실, 전원 장애의 SQLite 내구성은 별도 검증이 필요하다.
- DB fault fixture 이슈가 같은 theme에 누적되므로 matching 후보 수와 부하가 run 중 증가한다.
  초기 pilot도 같은 격리 스택을 사용했다. 정확한 ID는 run별 분리했으며 각 run의 후보 수는 Spark 원본 로그에 있다.
  이 workload로 운영 지속 TPS나 안정 상태의 지연 분포를 추정하지 않는다.
- fixture entrypoint는 전체 Collector round/heartbeat를 실행하지 않으며, lab NLP group 이름도 운영 기본과 다르다.
  따라서 다른 startup/round/NLP 경보의 정상·오탐률은 평가하지 않았다. 장시간 알림 측정은
  정확한 `CollectorDeliveryStalled` instance를 필터링한 결과다.
- 두 Backend와 모든 서비스가 같은 Docker VM 자원을 공유한다. 버전별 latency의 인과적 성능 비교는 하지 않는다.
- 3회 반복 및 한 번의 장시간 장애는 재현성 확인 자료다. 사건들이 독립 표본이라는 가정 없이
  무실패 관측을 99.9%/99.99% 가용성이나 일반적인 장애 확률로 바꾸지 않는다.

## 증거와 재실행

- [실험 코드와 실행 절차](../bigdata/reliability/README.md)
- [정식 결과](../bigdata/reliability/results/{primary.name}/summary.json)
- [집계와 제외 목록](../bigdata/reliability/results/{primary.name}/aggregate.json)
- [독립 ID/내용 검증](../bigdata/reliability/results/{primary.name}/verification.json)
- [보충 Collector 결과](../bigdata/reliability/results/{supplement.name}/summary.json)
- [환경·실제 이미지 ID](../bigdata/reliability/results/{primary.name}/environment.json)
- [최종 pipeline offset 대조](../bigdata/reliability/results/{primary.name}/pipeline-drained.json)

대용량 원본은 각 결과 디렉터리 `raw/`에 로컬 보존하고 Git에서는 제외한다.
Kafka 관측 JSONL, 입력 명세, JDI transcript, DB audit, 실제 HTTP 알림과 컨테이너 로그를
`evidence-sha256.json`으로 검증할 수 있다. 운영 서비스에는 배포하지 않았다.
"""
    if summary:
        delta_p95 = 100 * (summary["versions"]["after"]["p95_seconds"] / summary["versions"]["before"]["p95_seconds"] - 1)
        additions = ["\n### 별도 10,000건 burst 관측\n",
            "고정 입력을 Collector → Kafka → NLP transport → Spark → 두 Backend로 통과시켰다.\n"
            f"장애 실험의 누적 이슈와 분리한 {summary['themes']}개 theme × {summary['issue_candidates_per_theme']}개 활성 후보를 사용했다.\n"
            f"차원 비교 규모는 `10,000 × 20 × 768 = {summary['candidate_dimension_comparisons']:,}`이며, 이 catalog 규모에 한정한 관측이다.\n"
            f"실제 본문은 {summary['body_size_bytes']['min']:,}–{summary['body_size_bytes']['max']:,} bytes, raw JSON payload 합계는 {summary['raw_json_payload_bytes_total']:,} bytes ({summary['raw_json_payload_bytes_total']/2**20:.2f} MiB)였다.\n"
            "아래 지연은 수집 발견 시각부터 커밋된 최종 이슈 연결을 SQL로 최초 관측한 시간이며,\n"
            "0.5초 polling 및 query 시간이 포함된다. 같은 호스트의 쌍대 consumer 관측이다.\n",
            "| 버전 | 저장 / 입력 | 누락 / 추가 ID | 내용·연결 오류 | p50 / p95 / p99 (초) | 배치 완료율 (기사/초) |",
            "|---|---|---|---|---|---|"]
        for variant, item in summary["versions"].items():
            additions.append(f"| {variant} | {item['unique_persisted']:,}/{summary['input_articles']:,} | {item['missing_ids']} / {item['unexpected_ids']} | {item['payload_or_assignment_errors']} | {item['p50_seconds']:.2f} / {item['p95_seconds']:.2f} / {item['p99_seconds']:.2f} | {item['batch_completion_articles_per_second']:.2f} |")
        additions.append("\n배치 완료율은 `입력 기사 수 / 마지막 최종 연결 관측 지연`이다. 지속 TPS나 최대 용량이 아니다.\n"
                         f"실제 raw Kafka 고유 기사 {summary['raw_kafka_unique_articles']:,}건, body SHA-256 불일치 {summary['raw_body_hash_mismatches']}건이었다.\n"
                         f"이번 관측에서 개선 버전 p95는 기존보다 **{delta_p95:.2f}% 높았다**. 이 성능 차이도 그대로 기록한다.\n"
                         "inbox의 추가 SQL 비용과 partition 소비 순서·공유 DB 자원 영향을 분리하려면 버전별 단독 반복 부하 실험이 필요하다.\n"
                         "따라서 신뢰성 개선과 함께 처리 지연은 추가 확인·개선 대상으로 남긴다.\n"
                         f"[burst 원본 요약](../bigdata/reliability/results/{soak.name}/summary.json)\n")
        body = body.replace("## 해석의 한계", "\n".join(additions) + "\n## 해석의 한계")
    (ROOT / "DOCS/PIPELINE_RELIABILITY_EXPERIMENT.md").write_text(body, encoding="utf-8")
    print("Wrote DOCS/PIPELINE_RELIABILITY_EXPERIMENT.md")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("primary", type=Path)
    parser.add_argument("supplement", type=Path)
    parser.add_argument("--soak", type=Path)
    args = parser.parse_args()
    write_report(args.primary, args.supplement, args.soak)
