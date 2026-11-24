# EC2 예상 비용 — 본실험 미실행

2026-11-24 확인. 서울 ap-northeast-2, Linux On-Demand, c7i.2xlarge 1대(8 vCPU/16 GiB), gp3 100 GB, public IPv4 1개를 가정한다. EC2 생성이나 유료 실험은 수행하지 않았다.

EC2 $0.4032/h + EBS $0.0127/h + IPv4 $0.0050/h = **약 $0.4209/h**. EBS 월 요금은 30일/720시간으로 비례 배분한 계산 가정이며, 볼륨을 유지하면 인스턴스 종료 후에도 스토리지 요금이 계속된다. 세금·환율·추가 전송 비용은 별도다.

|범위|본측정 횟수|모델 시간 h|예산 시간 범위 h|예산 USD 범위|
|---|---:|---:|---:|---:|
|hash_scaling_one_volume|8,190|6.4|4.2–17.5|$1.76–$7.38|
|all_partitioning_one_volume|19,890|15.5|8.8–41.4|$3.69–$17.41|
|hash_scaling_two_volumes|16,380|9.9|5.9–26.7|$2.50–$11.25|
|all_partitioning_two_volumes|39,780|23.7|12.9–62.7|$5.42–$26.40|

EC2에서 미니 테스트만 재현하는 경우 초기 이미지 빌드·설정까지 1–2시간을 예약하면 약 **$0.42–$0.84**다. 이는 시간 예산이며 실제 소요 시간 보장이 아니다.

## 계산 근거

군집화 CPU 비용은 별도 합성 보정 데이터에서 추정한 비음수 이차식+일차식이다. 테마를 분할하지 않고 partition별 예상 비용을 합산한 뒤 Core 개수만큼 Task slot에 순서대로 배치하여 critical path를 근사했다. Spark·shuffle·반환 경로의 잔여 시간은 새 미니 테스트의 동일 Core·파티션·전략에서 관측한 값으로 보정했다. 입력량 증가에 대해 잔여 비용 고정/선형 증가의 중간값을 사용했다. AS-IS의 잔여 비용은 테마 수에 비례시켰다.

이 값은 로컬 미니 데이터에서 미측정한 960·1,920건과 EC2 CPU로 외삽한 것이다. 0.5배/2배 시간 시나리오와 추가 30% 여유, 초기 준비 1시간을 적용했으며 통계적 신뢰구간이나 최대 청구액이 아니다. 실제 EC2에서 짧은 재검증 후 시간 예산을 다시 계산해야 한다.

Task 효과는 Core를 고정하고 P=1/2/4/8/16을 비교하고, Core 효과는 P를 고정하고 C=1/2/4를 비교한다. 비교군은 AS-IS, CONTROL, Hash, 기사 수 LPT, 예상 연산 비용 LPT다. Driver 동적 전환 정책은 없다. AS-IS/CONTROL은 군집화 파티션 수가 없으므로 P별로 중복 반복하지 않는다.

본실험 규모는 예산 가정이다. 각 총량에서 단일 테마 1조건 + 4/8/16/32테마 × 균등/중간/심한 편향 12조건 = 13개 데이터 조건이다. 중간/심한 편향은 최대 테마 비중 35%/65%다. 총 960·1,920건이면 모든 테마가 최소 8건을 유지하고 최대 테마도 2,000건 상한 이하다. 단일 테마를 여러 테마 수와 교차하는 불가능한 조합은 제외한다. 조건별 30회로 계산했으며 본실험은 아직 실행하지 않았다.

## 환경 선택과 범위

c7i.2xlarge의 8 vCPU/16 GiB에서 기존 실험 quota(Driver 1, Worker 최대 4, Master 0.5 vCPU 및 메모리 합계 약 9.5 GiB)를 유지한다. 나머지는 OS와 Docker 여유다. Spark Core는 이 환경에서 논리 vCPU 슬롯이며 물리 Core와 동일하다고 단정하지 않는다. CPU topology와 SMT 정보를 기록한다. ARM/Graviton, burstable 인스턴스, Spot은 이번 재현 환경의 CPU/아키텍처 또는 중단 변수를 추가하므로 이 견적에서 사용하지 않는다.

한 EC2의 컨테이너 간 실험이다. 다중 물리 EC2 네트워크 분산 실험은 별도 비용과 설계가 필요하다. SSH는 본인 IP로 제한하고 Spark UI 포트는 인터넷에 공개하지 않는 구성을 가정한다. 인터넷 비용 항목에는 NAT Gateway를 포함하지 않았다. 이미지 다운로드는 측정 구간 밖이며 요금 산정의 준비 시간에 포함한다.

## 공식 단가 출처

- [AWS EC2 공식 공개 단가 JSON](https://b0.p.awsstatic.com/pricing/2.0/meteredUnitMaps/ec2/USD/current/ec2.json) — 발행 2026-11-23T17:10:05Z, c7i.2xlarge rateCode C8YNDY6DZQZ2Z7Q4.JRTCKXETXF.6YS6EN2CT7.
- [AWS EBS 공식 공개 단가 JSON](https://b0.p.awsstatic.com/pricing/2.0/meteredUnitMaps/ec2/USD/current/ebs.json) — gp3 $0.0912/GB-month.
- [AWS public IPv4 요금](https://aws.amazon.com/vpc/pricing/) — $0.005/IP-hour.
- [AWS C7i 사양](https://aws.amazon.com/ec2/instance-types/c7i/).

단가 원본의 선택 항목과 발행 정보는 `aws_pricing_snapshot.json`, 전체 조건과 산식 입력은 `planned_conditions.csv`, 재계산 결과는 `budget_scenarios.csv`와 `cost_assumptions.json`에 보존했다.
