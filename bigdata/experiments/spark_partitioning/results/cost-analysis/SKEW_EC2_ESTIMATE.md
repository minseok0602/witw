# EC2 의도적 편향 실험 비용 추정

2026-11-24, 서울 Linux On-Demand. 새 공개 요금 응답과 검증된 로컬 편향 실측을 근거로 계산했다. 유료 AWS 자원은 생성하지 않았다.

## 승인 대상으로 제안하는 범위

A0/A1/A2 각30회와 B0/B1/B2 각30회, 총180회다. A4,000건·B2,000건,768차원,총 Executor4 Core·4파티션을 유지한다. EC2 Quick A3회+B5회(Driver 기준 포함)와 워밍업·독립 모델 재보정은 별도다. 기존720회 전체 분포 행렬,Driver 기준군240회,통합C는 이번 범위에 포함하지 않는다.

|자원|수량|시간당 USD|
|---|---:|---:|
|m7i.xlarge Head|1|0.2478|
|m7i.large Worker|2|각 0.1239|
|gp3 40+20+20GB|80GB|0.0101|
|공인 IPv4 Head만|1|0.0050|
|합계||**0.5107**|

1 USD=1,400원,VAT10% 가정 시 시간당 약 **787원**이다. 환율은 예산 가정이며 청구 환율이 아니다. gp3 월 단가는720시간으로 비례 배분했다. 같은 AZ의 private IP 통신을 사용하며 Worker 공인IP와 NAT Gateway를 만들지 않는다.

## 시간과 비용 시나리오

로컬 미니18회 컨테이너 총 실행은 5.32분이었다. 준비/워밍업 고정 구간을 분리해30회로 외삽하면180회는 로컬 기준 약 42.0분이다. A/B의 입력 준비·원본 참조 검증·결과 저장도 포함하며 주timer 합계만 외삽하지 않았다.

EC2 연산 시간은 로컬 외삽의0.5~2배,구축45~90분,Quick/보정9~15분,분석·회수15~30분을 더해 **1.5~3.7시간**으로 계획했다. 이는 신뢰구간이 아니며 CPU SMT·서버 통신·이미지 빌드에 따라 달라진다.

|항목|추정|
|---|---:|
|180회 예상 비용|약 1,180~3,872원|
|4시간 시간 기준 비용+예비비1,000원|약 4,146원|
|제안 승인 예산|**5,000원**|
|18회만 재현하는 경우,1~2시간 가정|약 787~2,573원|

상단 예상 비용에는 예비비1,000원을 더했다. 다운로드·짧은 복구·잔여 EBS 유지에 대비한 계획 금액이다. 계정의 무료크레딧·인터넷 전송 무료한도는 가정하지 않았다. 4시간 watchdog은 인스턴스를 정지하며 EBS는 계속 과금되므로,결과 회수 뒤 이번 CloudFormation StackId의 자원을 삭제한다. 이 시간 기준은 최대 청구액 보장이 아니다.

## 승인 후 실행 방법

아래는 검토할 명령이며 실행하지 않았다. 서울 Key Pair 이름,개인키 파일 경로,SSH 허용 공인IPv4/32가 필요하다. 개인키 내용을 채팅에 올리거나 서버에 복사하지 않는다.

```powershell
python bigdata/experiments/spark_partitioning/infra/deploy.py --execute --stack witw-partitioning-skew-approved --key-name KEYPAIR --ssh-key-file C:/path/key.pem --ssh-cidr PUBLIC_IP/32 --budget-hours 4 --max-budget-krw 5000 --contingency-krw 1000 --estimate bigdata/experiments/spark_partitioning/results/cost-analysis/SKEW_EC2_ESTIMATE.json
python bigdata/experiments/spark_partitioning/infra/run_suite.py bigdata/experiments/spark_partitioning/.runtime/witw-partitioning-skew-approved --scope deliberate-skew --execute
```

EC2 Quick 정합성·실제 Executor 할당 검증 및 관측 시간에 따른 남은 예산 재확인이 통과해야 본실험을 시작한다. 추가 비용이 예상되면 임의로 범위를 늘리지 않는다.

## 근거

- [AWS EC2 공개 요금 JSON](https://b0.p.awsstatic.com/pricing/2.0/meteredUnitMaps/ec2/USD/current/ec2.json) — 2026-11-23T17:10:05Z 발행, 2026-11-24T11:29:41.840342+00:00 수집.
- [AWS EBS 공개 요금 JSON](https://b0.p.awsstatic.com/pricing/2.0/meteredUnitMaps/ec2/USD/current/ebs.json) — 서울 gp3 $0.0912/GB-month.
- [AWS Public IPv4 요금](https://aws.amazon.com/vpc/pricing/) — $0.005/IP-hour.
- [로컬 편향 실측](../../docs/DELIBERATE_SKEW_RESULT.md),[단가 응답 스냅샷](SKEW_PRICING_SNAPSHOT.json),[계산 JSON](SKEW_EC2_ESTIMATE.json).
