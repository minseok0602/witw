# EC2 3대 파이프라인 장애 실험

기존 로컬 실험을 세 Linux 호스트에 분리한다. LLM 호출은 하지 않고 요약·분류·768차원 임베딩
결과를 고정 입력으로 제공한다. 실제 Kafka, SQLite outbox, Spark enriched stream, PostgreSQL inbox,
Prometheus, Alertmanager와 HTTP 수신기를 실행한다. Spark는 compute 호스트의 `local[2]`이고
checkpoint는 EBS 위 파일이다. HDFS·Spark 다중 worker·Kafka broker failover는 검증 범위에 포함하지 않는다.

| 역할 | 서비스 |
|---|---|
| infra | Kafka, PostgreSQL, Redis, Kafka exporter |
| compute | Collector, NLP 결과 주입기, Spark, Toxiproxy, 실험 제어기 |
| app | 기존/개선 Backend, Prometheus, Alertmanager, 실험용 웹훅 수신기 |

기본 사양은 서울 리전 `m7i.xlarge` 3대(대당 4 vCPU / 16GiB), root gp3 각 100GB다.
2026-11-23 확인 단가 및 환율 1,500원·VAT 10% 가정으로 8시간 약 10,512원이다.
이는 추정치이고 실제 청구액이 아니다. 고정 리소스의 최대 사용 시간을 8시간으로 제한하며
작업 종료 후에는 그 전에 자원을 제거한다.

## 실행

AWS CLI 인증은 로컬 운영자 머신에만 둔다. 별도 실험 프로필을 사용한다.

```powershell
aws login --profile witw-reliability
python -m unittest discover -s bigdata/reliability/ec2 -p test_support.py -v
python bigdata/reliability/ec2/launch.py plan --profile witw-reliability
python bigdata/reliability/ec2/launch.py package
python bigdata/reliability/ec2/launch.py provision
python bigdata/reliability/ec2/launch.py deploy
python bigdata/reliability/ec2/launch.py start
python bigdata/reliability/ec2/launch.py wait-suite
python bigdata/reliability/ec2/launch.py host-cycles
python bigdata/reliability/ec2/launch.py fetch
# 회수 파일과 검증 결과를 확인한 뒤, 이 실험의 자원만 제거한다.
python bigdata/reliability/ec2/launch.py destroy
```

`plan`과 `package`는 유료 AWS 자원을 만들지 않는다. `provision`은 인증 계정과 지역 vCPU 한도를
확인한 뒤 전용 VPC/subnet/IGW/security group/key pair/인스턴스를 생성한다. 기존 VPC나 서버를 수정하지 않는다.
SSH는 운영자 공인 IP의 /32에만 허용하고, 서비스 포트는 실험 security group 내부에서만 허용한다.
서비스 포트는 각 EC2의 사설 IP에 bind한다. IAM 키/로그인 캐시는 서버로 복사하지 않는다.
실험 제어용으로 새로 만든 SSH 키만 compute 노드에 복사한다.

실행 상태와 로컬 패키지는 `.generated/ec2-runs/<run>/`에 저장되며 Git에서 제외한다.
`latest.txt`가 가리키는 실행 대신 `--state <절대경로/state.json>`으로 특정 실행을 선택할 수 있다.
`state.json`에는 합성 실험 DB 비밀번호와 SSH 경로가 있으므로 공개하지 않는다.

## 관측과 단계

1. 기존 정상·중복·순서 역전·DB 무응답·Collector/DB Writer 커밋 경계 SIGKILL을 각각 3회 실행한다.
2. 운영 경보 임계값을 유지한 outbox TCP 장애와 실제 firing/resolved HTTP 수신을 별도로 관측한다.
3. broker 컨테이너 30초 중단과 Spark 부분 출력 이후 checkpoint 커밋 전 SIGKILL을 각각 3회 실행한다.
4. 기존/개선 consumer 중 하나씩만 실행해 10,000기사 부하를 버전별 3회 비교한다.
   순서는 before/after와 after/before로 교대한다. 비활성 consumer의 backfill은 측정 종료 후에 시작한다.
5. 외부 운영자 머신이 infra/app/compute EC2의 stop/start를 각각 3회 실행한다.
   compute 제어기까지 재부팅되므로 복구 시간은 외부 운영자의 monotonic clock으로 측정한다.

EC2 stop/start는 정상적인 OS 종료를 포함하는 재시작 실험이다. 물리 디스크 유실·강제 전원 단절과
동일하다고 주장하지 않는다. host trial은 재시작 전에 입력 명세와 SQLite outbox 1,000건을 확인하고,
재시작 후 Kafka 레코드의 본문 hash와 DB ID/내용/전체 벡터/이슈 연결을 대조한다.

### Grafana 관측

app 호스트에서 `/opt/witw/venv/bin/python /opt/witw/repo/bigdata/reliability/ec2/grafana.py start`를
실행하면 기존 실험 네트워크에 관측용 Grafana를 추가한다. CPU 0.25·메모리 384MiB로 제한하고
`127.0.0.1:39903`에만 바인딩한다. 파이프라인과 Prometheus/Alertmanager를 재시작하지 않는다.
관리자 비밀번호는 app의 `.generated/ec2/grafana/credentials.json`에 0600으로 생성한다.
화면 확인은 운영자 SSH 터널을 통해 수행한다. Grafana가 실행 중이면 `verify` 명령으로
대시보드의 실제 PromQL, 경보 규칙 조회, Grafana 경유 Alertmanager API 연결을 검증할 수 있다.
이 검증은 HTTP API 검증이며 브라우저 화면을 육안 검수했다는 뜻은 아니다.
추가 시점과 리소스 제한은 실험 환경 기록에 남기고 비교 성능 측정 내내 같은 구성을 유지한다.
보조 관측 결과는 `.generated/ec2/observability`에 저장하며 전체 `fetch`가 별도 아카이브로 회수한다.
`verify`는 대시보드 JSON과 Prometheus의 수집 상태·경보·파이프라인·Kafka·웹훅 지표를
원래 수집 시각을 유지한 range-selector JSON으로 내보낸다. 수집 주기는 15초다.
이는 선택 지표의 시계열 내보내기이며
Prometheus 전체 TSDB 백업이나 실행 중인 Grafana 사이트 보존은 아니다.
수집 공백은 보간하거나 채우지 않는다. 최종 결과 회수 전에 이 명령을 다시 실행한다.

```powershell
python bigdata/reliability/ec2/launch.py status
python bigdata/reliability/ec2/launch.py fetch --metadata-only
```

`wait-suite`는 요약 JSON을 주기적으로 로컬에 회수한다. 컨테이너 단계 완료와 EC2 host 단계 완료는
구분하며, 미완료 상태는 통과로 표시하지 않는다. 전체 원본은 `fetch`의 `evidence.tar.gz`로 회수한다.
로컬 실험 결과를 cloud 결과로 복사하지 않는다. 원본 결과 파일과 증거 hash는 새 실행에서 다시 생성한다.

## 비용 제한과 정리

최초 생성 때 절대 만료 시각을 정하고 각 호스트에 persistent systemd timer를 설치한다.
EC2 재부팅으로 기한이 연장되지 않는다. 이 기한의 OS poweroff는 EC2 terminate로 연결하며
root EBS의 DeleteOnTermination을 켠다. 따라서 만료 전 원본 회수를 끝내야 한다.
AWS stop/start API로 수행하는 실험은 이 OS shutdown 동작과 구분된다.

`destroy`는 state의 인스턴스와 VPC에 해당 실행의 `LabRun` 태그가 있는지 확인하고 제거한다.
서버 생성 실패 등으로 일부 리소스만 생겨도 state를 보존하고 해당 실행의 자원을 정리한다.
자동 만료로 서버가 종료된 경우에도 VPC 등 나머지 네트워크 자원은 `destroy`로 제거한다.

첫 배포의 정상 접속·데이터 처리·원격 제어가 검증되기 전에는 실험 결과를 주장하지 않는다.
