"""Cost scenarios for the intentional-skew A/B experiment; no AWS control APIs."""
import csv
import json
from pathlib import Path


def read(path):
    with path.open(encoding='utf-8-sig',newline='') as f:return list(csv.DictReader(f))


def main():
    lab=Path(__file__).resolve().parents[1];out=lab/'results/cost-analysis'
    prices=json.loads((out/'SKEW_PRICING_SNAPSHOT.json').read_text())
    details=[]
    for app in ['A','B']:
        run=lab/'results'/f'{app}-explore-deliberate-skew-v1'
        assert json.loads((run/'VALIDATED.json').read_text())['passed']
        samples=read(run/'resources-driver.csv');epochs=[float(r['epoch_s']) for r in samples]
        assert max(b-a for a,b in zip(epochs,epochs[1:]))<5
        jobs=read(run/'jobs.csv')
        start=min(float(j['submission_ms'])/1000 for j in jobs if j['group'].startswith('prepare-explore-'))
        fixed=start-epochs[0];variable=epochs[-1]-start
        details.append(dict(app=app,source=run.name,measured_gross_s=epochs[-1]-epochs[0],
                            fixed_startup_warmup_s=fixed,three_repeat_variable_s=variable,
                            projected30_local_s=fixed+10*variable))
    ec2=float(prices['m7i.xlarge']['price'])+2*float(prices['m7i.large']['price'])
    ebs=80*float(prices['gp3']['price'])/720;ipv4=.005;usd_h=ec2+ebs+ipv4;krw_h=usd_h*1400*1.1
    local_h=sum(r['projected30_local_s'] for r in details)/3600
    # 0.5x..2x reflects unmeasured CPU/network behavior, not statistical confidence.
    low=local_h*.5+.75+.15+.25
    high=local_h*2+1.5+.25+.5
    report=dict(scope='intentional-skew-only',region='ap-northeast-2',repetitions_per_strategy=30,
        main_measurements=180,baseline_measurements=0,ec2_quick_additional_measurements=8,
        instances={'m7i.xlarge':1,'m7i.large':2},executor_cores=4,partitions=4,
        ec2_usd_h=ec2,ebs_usd_h=ebs,ipv4_usd_h=ipv4,total_usd_h=usd_h,
        fx_assumption_krw_per_usd=1400,vat_multiplier=1.1,total_krw_h=krw_h,
        local_observations=details,local_projected180_h=local_h,
        estimated_low_h=low,estimated_upper_h=high,
        estimated_low_krw=low*krw_h,estimated_upper_krw=high*krw_h+1000,
        contingency_krw=1000,proposed_auto_stop_hours=4,
        deadline_cost_plus_contingency_krw=4*krw_h+1000,proposed_budget_krw=5000,
        mini18_duration_h_scenario=[1,2],mini18_krw_with_contingency=[krw_h,2*krw_h+1000],
        paid_resources_created=False,ec2_measured=False,
        exclusions=['original 720-run distribution matrix','240 extra Driver baselines','integration C'],
        note='Planning scenarios, not guaranteed cost or runtime. Stop leaves EBS billed; collect and destroy the owned stack.')
    assert high<4 and report['deadline_cost_plus_contingency_krw']<5000
    (out/'SKEW_EC2_ESTIMATE.json').write_text(json.dumps(report,indent=2))
    text=['# EC2 의도적 편향 실험 비용 추정','',
      '2026-11-24, 서울 Linux On-Demand. 새 공개 요금 응답과 검증된 로컬 편향 실측을 근거로 계산했다. 유료 AWS 자원은 생성하지 않았다.', '',
      '## 승인 대상으로 제안하는 범위','',
      'A0/A1/A2 각30회와 B0/B1/B2 각30회, 총180회다. A4,000건·B2,000건,768차원,총 Executor4 Core·4파티션을 유지한다. EC2 Quick A3회+B5회(Driver 기준 포함)와 워밍업·독립 모델 재보정은 별도다. 기존720회 전체 분포 행렬,Driver 기준군240회,통합C는 이번 범위에 포함하지 않는다.', '',
      '|자원|수량|시간당 USD|','|---|---:|---:|',
      f"|m7i.xlarge Head|1|{float(prices['m7i.xlarge']['price']):.4f}|",
      f"|m7i.large Worker|2|각 {float(prices['m7i.large']['price']):.4f}|",
      f'|gp3 40+20+20GB|80GB|{ebs:.4f}|','|공인 IPv4 Head만|1|0.0050|',
      f'|합계||**{usd_h:.4f}**|','',
      f'1 USD=1,400원,VAT10% 가정 시 시간당 약 **{krw_h:,.0f}원**이다. 환율은 예산 가정이며 청구 환율이 아니다. gp3 월 단가는720시간으로 비례 배분했다. 같은 AZ의 private IP 통신을 사용하며 Worker 공인IP와 NAT Gateway를 만들지 않는다.', '',
      '## 시간과 비용 시나리오','',
      f"로컬 미니18회 컨테이너 총 실행은 {sum(r['measured_gross_s'] for r in details)/60:.2f}분이었다. 준비/워밍업 고정 구간을 분리해30회로 외삽하면180회는 로컬 기준 약 {local_h*60:.1f}분이다. A/B의 입력 준비·원본 참조 검증·결과 저장도 포함하며 주timer 합계만 외삽하지 않았다.", '',
      f'EC2 연산 시간은 로컬 외삽의0.5~2배,구축45~90분,Quick/보정9~15분,분석·회수15~30분을 더해 **{low:.1f}~{high:.1f}시간**으로 계획했다. 이는 신뢰구간이 아니며 CPU SMT·서버 통신·이미지 빌드에 따라 달라진다.', '',
      '|항목|추정|','|---|---:|',
      f'|180회 예상 비용|약 {low*krw_h:,.0f}~{high*krw_h+1000:,.0f}원|',
      f'|4시간 시간 기준 비용+예비비1,000원|약 {4*krw_h+1000:,.0f}원|',
      '|제안 승인 예산|**5,000원**|',
      f'|18회만 재현하는 경우,1~2시간 가정|약 {krw_h:,.0f}~{2*krw_h+1000:,.0f}원|','',
      '상단 예상 비용에는 예비비1,000원을 더했다. 다운로드·짧은 복구·잔여 EBS 유지에 대비한 계획 금액이다. 계정의 무료크레딧·인터넷 전송 무료한도는 가정하지 않았다. 4시간 watchdog은 인스턴스를 정지하며 EBS는 계속 과금되므로,결과 회수 뒤 이번 CloudFormation StackId의 자원을 삭제한다. 이 시간 기준은 최대 청구액 보장이 아니다.', '',
      '## 승인 후 실행 방법','',
      '아래는 검토할 명령이며 실행하지 않았다. 서울 Key Pair 이름,개인키 파일 경로,SSH 허용 공인IPv4/32가 필요하다. 개인키 내용을 채팅에 올리거나 서버에 복사하지 않는다.', '',
      '```powershell',
      'python bigdata/experiments/spark_partitioning/infra/deploy.py --execute --stack witw-partitioning-skew-approved --key-name KEYPAIR --ssh-key-file C:/path/key.pem --ssh-cidr PUBLIC_IP/32 --budget-hours 4 --max-budget-krw 5000 --contingency-krw 1000 --estimate bigdata/experiments/spark_partitioning/results/cost-analysis/SKEW_EC2_ESTIMATE.json',
      'python bigdata/experiments/spark_partitioning/infra/run_suite.py bigdata/experiments/spark_partitioning/.runtime/witw-partitioning-skew-approved --scope deliberate-skew --execute',
      '```','',
      'EC2 Quick 정합성·실제 Executor 할당 검증 및 관측 시간에 따른 남은 예산 재확인이 통과해야 본실험을 시작한다. 추가 비용이 예상되면 임의로 범위를 늘리지 않는다.', '',
      '## 근거','',
      f"- [AWS EC2 공개 요금 JSON]({prices['sources']['ec2']}) — {prices['ec2_publication']} 발행, {prices['retrieved_utc']} 수집.",
      f"- [AWS EBS 공개 요금 JSON]({prices['sources']['ebs']}) — 서울 gp3 $0.0912/GB-month.",
      '- [AWS Public IPv4 요금](https://aws.amazon.com/vpc/pricing/) — $0.005/IP-hour.',
      '- [로컬 편향 실측](../../docs/DELIBERATE_SKEW_RESULT.md),[단가 응답 스냅샷](SKEW_PRICING_SNAPSHOT.json),[계산 JSON](SKEW_EC2_ESTIMATE.json).','']
    (out/'SKEW_EC2_ESTIMATE.md').write_text('\n'.join(text),encoding='utf-8')
    print(json.dumps(report,indent=2))


if __name__=='__main__':main()
