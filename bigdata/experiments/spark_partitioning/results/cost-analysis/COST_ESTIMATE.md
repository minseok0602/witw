# AWS 비용 추정 — 유료 자원 미생성

서울 On-Demand: m7i.xlarge 1대 $0.2478/h + m7i.large 2대 각각 $0.1239/h. EC2 합계 $0.4956/h.
gp3 40+20+20=80 GB $0.0101/h, Head public IPv4 1개 $0.005/h를 포함하면 **$0.5107/h**다. Worker public IPv4와 NAT Gateway는 없다. 같은 AZ의 private IP로 통신한다.

|항목|범위/금액|
|---|---:|
|A/B 주 본실험|720회|
|별도 AS-IS/CONTROL|240회|
|로컬 탐색 기반 A/B 전체 시간 외삽|2.21 h|
|EC2 준비·검증·분석·C 예약 포함 시간 시나리오|2.6–7.9 h|
|예상 비용, 1 USD=1,400원 및 VAT 10% 가정|약 2,047–9,223원|
|제안한 10시간 자동중지 상한 + 예비비|약 10,865원|
|목표 예산|30,000원|

상단 비용 범위의 상한에는 예비비 3,000원을 포함했다. 환율 1,400원은 예산 산정 가정이며 AWS 청구 환율이 아니다. EBS 월 요금은 720시간으로 비례 배분했다. 종료 후 볼륨을 보존하면 비용이 계속되므로 결과 회수 후 이 스택만 제거한다. 예비비는 짧은 복구·결과 다운로드·스토리지 유지에 대비한 계획 금액이며 통계적 오차 범위가 아니다.

## 실측과 추정의 구분

탐색은 A 주요 전략 36회와 B 주요+기준군 60회를 로컬 두 Worker/두 Executor에서 실행했다. 정상 완료한 실행만 시간 산정에 사용했다. 각 app의 실제 container 시작~종료 시간에서 초기 준비/워밍업/보정 부분을 분리하고, 3회 반복의 나머지 시간을 30회로 외삽했다. 이 계산에는 timer 밖의 입력 생성·체크포인트 준비·공통 집계·독립 참조 비교·CSV 저장도 포함된다.

EC2 CPU의 SMT 구조, 물리 서버 간 네트워크 및 잡음은 아직 측정하지 않았다. 로컬 외삽값의 0.5배와 2배를 시간 시나리오로 사용하고 준비 1시간·EC2 Quick 0.25시간·분석/회수 0.25시간을 더했다. C는 미실행이며 상단 시나리오에 별도 2시간을 예약했을 뿐이다. EC2 Quick 후 실제 관측으로 예산을 다시 확인하고 초과가 예상되면 본실험을 시작하지 않는다.

고정 랜덤 seed로 테마별 기사 수 배치를 바꾸므로 A의 총 유사도 비교 횟수는 반복마다 다를 수 있다. 3개 seed 외삽은 이 차이를 충분히 대표하지 못할 수 있다. 이 추정은 최대 청구액 보장이나 성능 신뢰구간이 아니다.

10시간 watchdog은 해당 CloudFormation StackId 태그의 세 인스턴스만 정지하고 EBS의 원시 결과는 보존한다. 정지는 삭제가 아니므로 잔여 EBS 비용은 계속된다. 결과 회수 후 collect_destroy.py로 같은 스택을 제거하는 절차를 포함한다. 준비·실행 실패도 로그를 보존하고 새 실험 ID로 재시도해야 하며, 기존 실패 표본을 덮어쓰지 않는다.

## 공식 요금 근거

- [EC2 공개 단가 JSON](https://b0.p.awsstatic.com/pricing/2.0/meteredUnitMaps/ec2/USD/current/ec2.json) — 2026-11-23T17:10:05Z 발행, m7i.xlarge/m7i.large Linux On-Demand 서울.
- [EBS 공개 단가 JSON](https://b0.p.awsstatic.com/pricing/2.0/meteredUnitMaps/ec2/USD/current/ebs.json) — 서울 gp3 $0.0912/GB-month.
- [Public IPv4](https://aws.amazon.com/vpc/pricing/) — $0.005/IP-hour.
- [M7i 사양](https://aws.amazon.com/ec2/instance-types/m7i/).

견적은 EC2 인스턴스, 기본 gp3, IPv4 및 명시한 예비비에 한정한다. Savings Plan·Spot·무료 크레딧은 가정하지 않는다. 유료 자원 생성에는 사용자 승인이 필요하며, 이 보고서 생성은 AWS 계정 제어 API를 호출하지 않는다.
