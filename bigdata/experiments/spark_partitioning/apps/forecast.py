"""Budget extrapolation from actual, validated local exploration. No AWS API calls."""
import argparse
import csv
import json
import random
import statistics
from pathlib import Path


def read(path):
    with Path(path).open(encoding='utf-8-sig',newline='') as f:return list(csv.DictReader(f))


def main(root):
    results=root/'results';out=results/'cost-analysis';out.mkdir(exist_ok=True)
    details=[]
    for app,name in [('A','A-explore-v2'),('B','B-explore-v3')]:
        run=results/name
        validation=json.loads((run/'VALIDATED.json').read_text());assert validation['passed']
        samples=read(run/'resources-driver.csv')
        gaps=[float(b['epoch_s'])-float(a['epoch_s']) for a,b in zip(samples,samples[1:])]
        if max(gaps)>5:raise ValueError('Interrupted run cannot be a timing basis')
        gross=float(samples[-1]['epoch_s'])-float(samples[0]['epoch_s'])
        jobs=read(run/'jobs.csv')
        start=min(float(j['submission_ms'])/1000 for j in jobs if j['group'].startswith('prepare-explore-'))
        fixed=max(0,start-float(samples[0]['epoch_s']))
        variable=max(0,gross-fixed)
        observed=read(run/'runs.csv');measured=[r for r in observed if r['phase']=='explore']
        details.append(dict(app=app,source=name,gross_local_s=gross,startup_warmup_calibration_s=fixed,
            variable_three_repeat_s=variable,full30_local_equivalent_s=fixed+10*variable,
            main_runs=360,extra_baseline_runs=240 if app=='B' else 0,
            measured_main_wall_s=sum(float(r['wall_s']) for r in measured if r['mode'] in (['A0','A1','A2'] if app=='A' else ['B0','B1','B2'])),
            measured_extra_baseline_wall_s=sum(float(r['wall_s']) for r in measured if r['mode'] in ['AS_IS','CONTROL'])))
    # The full command uses the exact same setup, per-case preparation, reference checks and materialization.
    local_full_h=sum(x['full30_local_equivalent_s'] for x in details)/3600
    prices=json.loads((root/'infra/pricing_snapshot.json').read_text())
    ec2=float(prices['instances']['m7i.xlarge']['price'])+2*float(prices['instances']['m7i.large']['price'])
    ebs=80*float(prices['gp3']['price'])/720;ipv4=.005;rate=ec2+ebs+ipv4
    fx=1400;vat=1.1
    # C is not timed locally. It is an explicit planning reservation, not a measurement.
    c_reserve=2.;setup=1.;analysis=.25;quick=.25
    low_h=setup+analysis+quick+.5*local_full_h
    high_h=setup+analysis+quick+2*local_full_h+c_reserve
    low_krw=low_h*rate*fx*vat
    high_krw=high_h*rate*fx*vat+3000
    deadline=10
    cap_krw=deadline*rate*fx*vat+3000
    report=dict(region='ap-northeast-2',instances={'m7i.xlarge':1,'m7i.large':2},main_runs=720,
        extra_baseline_runs=240,total_measured_full_runs=960,local_exploration=details,
        local_extrapolated_ab_full_h=local_full_h,estimated_low_h=low_h,estimated_high_h=high_h,
        ec2_usd_h=ec2,ebs_80gb_usd_h=ebs,ipv4_usd_h=ipv4,total_usd_h=rate,
        fx_assumption=fx,vat_assumption=vat,contingency_krw=3000,c_integration_reserved_h=c_reserve,
        estimated_low_krw=low_krw,estimated_upper_krw=high_krw,budget_krw=30000,
        proposed_auto_stop_hours=deadline,deadline_cost_plus_contingency_krw=cap_krw,
        paid_resources_created=False,ec2_experiment_executed=False,c_experiment_executed=False,
        caveat='0.5x/2x local runtime scenarios, not a confidence interval. Data rotations, SMT, real network and C behavior remain uncertain.')
    assert high_krw<30000 and cap_krw<30000
    (out/'COST_ESTIMATE.json').write_text(json.dumps(report,indent=2))
    text=['# AWS 비용 추정 — 유료 자원 미생성','',
      f"서울 On-Demand: m7i.xlarge 1대 ${float(prices['instances']['m7i.xlarge']['price']):.4f}/h + m7i.large 2대 각각 ${float(prices['instances']['m7i.large']['price']):.4f}/h. EC2 합계 ${ec2:.4f}/h.",
      f'gp3 40+20+20=80 GB ${ebs:.4f}/h, Head public IPv4 1개 $0.005/h를 포함하면 **${rate:.4f}/h**다. Worker public IPv4와 NAT Gateway는 없다. 같은 AZ의 private IP로 통신한다.', '',
      '|항목|범위/금액|','|---|---:|',
      '|A/B 주 본실험|720회|','|별도 AS-IS/CONTROL|240회|',
      f'|로컬 탐색 기반 A/B 전체 시간 외삽|{local_full_h:.2f} h|',
      f'|EC2 준비·검증·분석·C 예약 포함 시간 시나리오|{low_h:.1f}–{high_h:.1f} h|',
      f'|예상 비용, 1 USD=1,400원 및 VAT 10% 가정|약 {low_krw:,.0f}–{high_krw:,.0f}원|',
      f'|제안한 10시간 자동중지 상한 + 예비비|약 {cap_krw:,.0f}원|',
      '|목표 예산|30,000원|','',
      '상단 비용 범위의 상한에는 예비비 3,000원을 포함했다. 환율 1,400원은 예산 산정 가정이며 AWS 청구 환율이 아니다. EBS 월 요금은 720시간으로 비례 배분했다. 종료 후 볼륨을 보존하면 비용이 계속되므로 결과 회수 후 이 스택만 제거한다. 예비비는 짧은 복구·결과 다운로드·스토리지 유지에 대비한 계획 금액이며 통계적 오차 범위가 아니다.', '',
      '## 실측과 추정의 구분','',
      '탐색은 A 주요 전략 36회와 B 주요+기준군 60회를 로컬 두 Worker/두 Executor에서 실행했다. 정상 완료한 실행만 시간 산정에 사용했다. 각 app의 실제 container 시작~종료 시간에서 초기 준비/워밍업/보정 부분을 분리하고, 3회 반복의 나머지 시간을 30회로 외삽했다. 이 계산에는 timer 밖의 입력 생성·체크포인트 준비·공통 집계·독립 참조 비교·CSV 저장도 포함된다.', '',
      'EC2 CPU의 SMT 구조, 물리 서버 간 네트워크 및 잡음은 아직 측정하지 않았다. 로컬 외삽값의 0.5배와 2배를 시간 시나리오로 사용하고 준비 1시간·EC2 Quick 0.25시간·분석/회수 0.25시간을 더했다. C는 미실행이며 상단 시나리오에 별도 2시간을 예약했을 뿐이다. EC2 Quick 후 실제 관측으로 예산을 다시 확인하고 초과가 예상되면 본실험을 시작하지 않는다.', '',
      '고정 랜덤 seed로 테마별 기사 수 배치를 바꾸므로 A의 총 유사도 비교 횟수는 반복마다 다를 수 있다. 3개 seed 외삽은 이 차이를 충분히 대표하지 못할 수 있다. 이 추정은 최대 청구액 보장이나 성능 신뢰구간이 아니다.', '',
      '10시간 watchdog은 해당 CloudFormation StackId 태그의 세 인스턴스만 정지하고 EBS의 원시 결과는 보존한다. 정지는 삭제가 아니므로 잔여 EBS 비용은 계속된다. 결과 회수 후 collect_destroy.py로 같은 스택을 제거하는 절차를 포함한다. 준비·실행 실패도 로그를 보존하고 새 실험 ID로 재시도해야 하며, 기존 실패 표본을 덮어쓰지 않는다.', '',
      '## 공식 요금 근거','',
      f"- [EC2 공개 단가 JSON]({prices['sources']['ec2']}) — {prices['publication']} 발행, m7i.xlarge/m7i.large Linux On-Demand 서울.",
      f"- [EBS 공개 단가 JSON]({prices['sources']['ebs']}) — 서울 gp3 ${float(prices['gp3']['price'])}/GB-month.",
      '- [Public IPv4](https://aws.amazon.com/vpc/pricing/) — $0.005/IP-hour.',
      '- [M7i 사양](https://aws.amazon.com/ec2/instance-types/m7i/).','',
      '견적은 EC2 인스턴스, 기본 gp3, IPv4 및 명시한 예비비에 한정한다. Savings Plan·Spot·무료 크레딧은 가정하지 않는다. 유료 자원 생성에는 사용자 승인이 필요하며, 이 보고서 생성은 AWS 계정 제어 API를 호출하지 않는다.']
    (out/'COST_ESTIMATE.md').write_text('\n'.join(text)+'\n',encoding='utf-8')
    print(json.dumps(report,indent=2))


if __name__=='__main__':
    main(Path(__file__).resolve().parents[1])
