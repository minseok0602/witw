# EC2 실행과 결과 보존

현재 승인된540회·예산10,000원 실행은 [APPROVED_RELIABILITY_RUN.md](../docs/APPROVED_RELIABILITY_RUN.md)를 따른다. `run_reliability.py`가 두 세션,회수,삭제를 제어하고 `finalize_local.py`가 완료 후 보고서를 생성한다. 아래180회 및720회 명령은 다른 범위이므로 혼동하지 않는다.

의도적 편향 조건만 재현하는 최신 제안은 [180회/예산5,000원 견적](../results/cost-analysis/SKEW_EC2_ESTIMATE.md)이다. 이 경우 `--budget-hours 4 --max-budget-krw 5000 --contingency-krw 1000` 및 suite의 `--scope deliberate-skew`를 사용한다. 아래 원래 기본 명령은720회+별도기준240회이므로 두 실행 범위를 혼동하지 않는다. 이 제안도 아직 승인·실행 전이다.

2026-11-24 현재 **유료 자원을 생성하지 않았다.** 다음 명령은 사용자 승인 후 실행한다. 승인 대상은 서울 On-Demand m7i.xlarge 1대와 m7i.large 2대, 같은 AZ, 총 Executor 4 Core, gp3 80GB, Head 공인 IPv4 1개, 기본 10시간 자동 정지다. [비용 보고서](../results/cost-analysis/COST_ESTIMATE.md)의 환율·VAT·예비비 가정을 함께 검토한다.

## 준비된 범위

CloudFormation 템플릿, 전용 VPC/서브넷, SSH /32 및 클러스터 내부 보안 그룹, 고정 자원 Docker bootstrap, A/B Quick→예산 재확인→30회 반복, 수집·종료 스크립트를 작성했다. 템플릿은 cfn-lint 오프라인 검증을 통과했다. 실제 AMI 부팅·계정 quota·선택 AZ의 가용성·bootstrap·물리 네트워크 성능은 아직 EC2에서 검증하지 않았다. 사용자 승인 후 Quick Test에서 확인한다.

Worker에는 NAT Gateway와 공인 IP가 없다. Head에서 프로젝트 Spark 이미지와 실험 이미지를 빌드하고 이미지·코드를 private IP HTTP로 전달한다. Worker는 SHA256과 이미지 ID를 확인한다. Head만 패키지 다운로드용 인터넷을 사용한다. Spark/결과 HTTP 포트는 같은 보안 그룹 내부에만 허용하며 외부 SSH는 지정 /32로 제한한다.

gp3는 Head40/Worker20/20GB, 3000 IOPS, 기본 throughput125MiB/s, 암호화, 종료 시 삭제다. 생성 후 DescribeInstances/Volumes로 실제 유형·AZ·스토리지 설정을 검사해 `actual_infrastructure.json`에 남긴다. 실행 후에는 실제 ExecutorAdded=[2,2]와 연산 Stage 4Task를 다시 검사한다.

## 승인 후 명령

컨트롤러에는 AWS CLI, CloudFormation/EC2/IAM 관리 권한, OpenSSH가 필요하다. 서울 리전의 기존 EC2 Key Pair 이름·그 개인키 파일 경로·접속자의 공인 IPv4/32를 사용한다. 개인키 내용은 채팅에 올리거나 서버에 복사하지 않는다.

```powershell
# 기본 동작은 로컬 계획 출력이며 AWS API를 호출하지 않는다.
python bigdata/experiments/spark_partitioning/infra/deploy.py

# 사용자 승인 후에만 아래 --execute 사용
python bigdata/experiments/spark_partitioning/infra/deploy.py --execute --stack witw-partitioning-approved-run --key-name YOUR_SEOUL_KEYPAIR --ssh-key-file C:/path/to/key.pem --ssh-cidr YOUR_PUBLIC_IPV4/32 --budget-hours 10 --estimate bigdata/experiments/spark_partitioning/results/cost-analysis/COST_ESTIMATE.json

# 생성된 소유권 디렉터리. 기본은 계획 출력
python bigdata/experiments/spark_partitioning/infra/run_suite.py bigdata/experiments/spark_partitioning/.runtime/witw-partitioning-approved-run
# 승인된 클러스터에서만 실제 Quick A/B → 예산 재확인 → Full A/B → 결과 회수 → 스택 삭제
python bigdata/experiments/spark_partitioning/infra/run_suite.py bigdata/experiments/spark_partitioning/.runtime/witw-partitioning-approved-run --execute
```

`--profile`은 deploy에서 선택 가능하며 소유권 파일에 기록돼 후속 스크립트가 재사용한다. CLI의 `--execute` 자체가 대화상 사용자 승인을 대신하지 않는다. 승인받지 않은 인스턴스 유형·시간·예산으로 바꾸지 않는다.

EC2 Quick의 실제 컨테이너 시간을 사용해 30회 시간을 외삽하고 2배 시간 시나리오+1시간 회수 예약이 watchdog 잔여 시간 내인지 검사한다. 통과하지 않으면 본실험을 시작하지 않는다. B Full은 EC2 Quick에서 만든 독립 보정 모델을 동결해 사용한다. 전체 A/B 720회 외에 AS_IS/CONTROL 240회가 추가된다. C는 자동 실행하지 않는다.

## 실패·정지·수집

`deploy.py` 실패 시 해당 StackId 소유 태그의 실행 중 노드만 정지하고 소유권·실패 파일을 남긴다. CF 생성이 롤백되면 해당 스택 정책에 따라 생성 자원이 제거될 수 있다. 본실험 실패 시 `run_suite.py`도 결과를 회수한 뒤 기본적으로 같은 스택을 제거한다. 회수 실패 시 스택을 삭제하지 않고 소유 노드만 정지해 EBS를 보존한다.

Head의 systemd watchdog은 부팅 후 10시간에 해당 StackId의 인스턴스만 정지한다. 이는 청구 보장 상한이나 자원 삭제가 아니다. OS/IAM 장애 시 watchdog이 실패할 수 있으며 EBS는 계속 과금된다. Head를 재부팅하면 OnBootSec 기준 시간이 다시 시작되므로 실험 중 임의 재시작하지 않는다. 정지 후 회수는 예비비 안의 짧은 복구에만 사용한다.

```powershell
# 기본은 계획만 출력
python bigdata/experiments/spark_partitioning/infra/collect_destroy.py bigdata/experiments/spark_partitioning/.runtime/witw-partitioning-approved-run
# 결과 tar.gz와 SHA256 영수증 저장 후 정확히 이 스택만 삭제
python bigdata/experiments/spark_partitioning/infra/collect_destroy.py bigdata/experiments/spark_partitioning/.runtime/witw-partitioning-approved-run --execute --destroy
# watchdog으로 정지된 경우. 소유 노드만 일시 재시작해 회수하고 삭제
python bigdata/experiments/spark_partitioning/infra/collect_destroy.py bigdata/experiments/spark_partitioning/.runtime/witw-partitioning-approved-run --execute --recover-stopped --destroy
```

수집기는 Stack ARN 및 세 인스턴스 OwnerStackId를 검사한다. tar 수집·해시 영수증 생성이 성공해야 delete-stack을 호출한다. 회수용으로 재시작한 노드는 회수 실패 시 다시 정지한다. 다른 AWS 자원이나 운영 데이터를 열거해 삭제하지 않는다. `.runtime`에는 계정 식별자·키 경로·인프라 정보가 있으므로 공개 저장소에 커밋하지 않는다.

## C 후속 작업

A/B 본실험 분석 후 정당한 전략을 선정하면 `integration/plan.py`가 C0~C3, app당 최대2 Core·executor당1 Core 계약을 생성한다. 현재는 AST adapter와 고정 LLM stub의 정적 테스트까지만 완료했다. 실제 C 서비스 배포·UUID fixture·DB/HDFS 상태 복원·E2E 실행/계측은 추가 구현이 필요하다. Java DB Writer가 `news.event.v1`을 고정 구독하므로 매 케이스 별도 Kafka 저장소와 동일 literal topic을 사용해야 한다. A/B 비용 시나리오에 더한 C 2시간은 예약 가정이며 C 측정 시간이나 완성된 실행기를 의미하지 않는다.
