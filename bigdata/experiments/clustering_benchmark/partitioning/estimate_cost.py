"""Read only local mini results plus public AWS prices. Never calls an AWS control API."""
import argparse
import csv
import heapq
import json
import statistics
from collections import defaultdict
from pathlib import Path
import numpy as np
from placement import placement,predicted_cost


def read(path):
    with Path(path).open(encoding='utf-8-sig',newline='') as f:
        return list(csv.DictReader(f))


def write(path,rows):
    with Path(path).open('w',newline='') as f:
        w=csv.DictWriter(f,fieldnames=list(rows[0]));w.writeheader();w.writerows(rows)


def schedule(loads,cores):
    slots=[0.]*cores
    heapq.heapify(slots)
    for load in loads:
        t=heapq.heappop(slots);heapq.heappush(slots,t+load)
    return max(slots,default=0.)


def counts_for(total,themes,distribution):
    if themes==1:
        return [total]
    if distribution=='uniform':
        q,r=divmod(total,themes)
        return [q+(i<r) for i in range(themes)]
    first=round(total*({'moderate':.35,'severe':.65}[distribution]))
    q,r=divmod(total-first,themes-1)
    return [first]+[q+(i<r) for i in range(themes-1)]


def estimate(paths,out,pricing_path):
    out.mkdir(parents=True,exist_ok=True)
    envs=[json.loads((p/'environment.json').read_text()) for p in paths]
    assert {e['cores'] for e in envs}=={1,2,4} and len(envs)==3
    assert len({e['benchmark_sha256'] for e in envs})==1
    assert len({e['placement_sha256'] for e in envs})==1
    assert len({e['cost_model_sha256'] for e in envs})==1
    runs=[];parts=[]
    for path in paths:
        check=json.loads((path/'VALIDATED.json').read_text())
        assert check['passed'] and check['full_experiment_run'] is False
        runs+=read(path/'runs.csv');parts+=read(path/'partition_runs.csv')
    measured=[r for r in runs if r['phase']!='warmup']
    inputs=defaultdict(set)
    for r in measured:
        inputs[(r['phase'],r['scenario'],r['seed'])].add((r['input_digest'],r['digest']))
    assert all(len(s)==1 for s in inputs.values())
    model=json.loads((paths[0]/'cost_model.json').read_text())
    overhead=defaultdict(list);serial=defaultdict(list)
    for r in runs:
        if r['phase']!='mini':
            continue
        c,p,m=int(r['cores']),int(r['partitions']),r['mode']
        if m.startswith('to_be_'):
            pp=sorted([x for x in parts if x['group']==r['group']],key=lambda x:int(x['partition']))
            critical=schedule([float(x['kernel_wall_s']) for x in pp],c)
            residual=max(0,float(r['wall_s'])-critical)
            overhead[c,p,m].append((int(r['selected_articles']),residual))
        else:
            residual=max(0,float(r['wall_s'])-float(r['kernel_sum_s']))
            serial[c,m].append((int(r['selected_articles']),int(r['themes']),residual))
    prices=json.loads(pricing_path.read_text(encoding='utf-8'))
    ec2_rate=float(prices['ec2']['c7i.2xlarge']['price'])
    ebs_gb_month=float(prices['ebs_gp3']['price'])
    # Explicit budget convention: 100 GB gp3, 30-day/720h month, one public IPv4.
    rate=ec2_rate+100*ebs_gb_month/720+prices['public_ipv4_usd_per_hour']
    records=[]
    for total in (960,1920):
        for themes in (1,4,8,16,32):
            distributions=['single'] if themes==1 else ['uniform','moderate','severe']
            for dist in distributions:
                counts=counts_for(total,themes,dist)
                assert min(counts)>=8 and max(counts)<=2000 and sum(counts)==total
                countmap=dict(enumerate(counts,1))
                for c in (1,2,4):
                    for m in ('as_is','control','to_be_1','to_be_2','to_be_3'):
                        for p in ([0] if m in ('as_is','control') else [1,2,4,8,16]):
                            cpu=sum(predicted_cost(n,model) for n in counts)
                            if p:
                                mapping=placement(countmap,p,m,model)
                                loads=[sum(predicted_cost(countmap[t],model) for t in countmap if mapping[t]==pid) for pid in range(p)]
                                compute=schedule(loads,c)
                                samples=overhead[c,p,m]
                                # Two transparent bracketing assumptions, fixed vs linearly growing residual.
                                fixed=statistics.median(y for n,y in samples)
                                linear=statistics.median(y*total/n for n,y in samples)
                                overhead_s=(fixed+linear)/2
                            else:
                                compute=cpu
                                samples=serial[c,m]
                                if m=='as_is':
                                    overhead_s=statistics.median(y*themes/t for n,t,y in samples)
                                else:
                                    fixed=statistics.median(y for n,t,y in samples)
                                    linear=statistics.median(y*total/n for n,t,y in samples)
                                    overhead_s=(fixed+linear)/2
                            seconds=compute+overhead_s
                            records.append(dict(total_articles=total,themes=themes,distribution=dist,cores=c,
                                mode=m,partitions=p,repetitions=30,estimated_compute_s=compute,
                                estimated_residual_s=overhead_s,estimated_run_s=seconds,estimated_condition_s=seconds*30))
    write(out/'planned_conditions.csv',records)
    scenarios=[]
    for volumes,label in [([1920],'one_volume'),([960,1920],'two_volumes')]:
        for modes,name in [({'as_is','control','to_be_1'},'hash_scaling'),
                           ({'as_is','control','to_be_1','to_be_2','to_be_3'},'all_partitioning')]:
            rr=[r for r in records if r['total_articles'] in volumes and r['mode'] in modes]
            modeled=sum(r['estimated_condition_s'] for r in rr)/3600
            # Bounds are planning scenarios, not statistical intervals or EC2 measurements.
            low=1+modeled*.5
            high=1+modeled*2*1.3
            scenarios.append(dict(scenario=name+'_'+label,total_articles=';'.join(map(str,volumes)),
                conditions=len(rr),measured_runs=sum(r['repetitions'] for r in rr),
                local_model_hours=modeled,budget_low_hours=low,budget_high_hours=high,
                budget_low_usd=low*rate,budget_high_usd=high*rate))
    write(out/'budget_scenarios.csv',scenarios)
    assumptions=dict(region='ap-northeast-2',instance='c7i.2xlarge',instances=1,
        compute_usd_per_hour=ec2_rate,ebs_gp3_gb=100,ebs_gb_month_usd=ebs_gb_month,
        ebs_month_hours_assumption=720,public_ipv4=1,total_budget_usd_per_hour=rate,
        calibration_model=model,mini_samples=len(measured),
        full_experiment_run=False,ec2_resources_created=False,
        execution_time_status='Unvalidated local-model extrapolation, not an EC2 timing measurement',
        low_assumption='one hour setup + 0.5 times modeled runtime',
        high_assumption='one hour setup + 2 times modeled runtime + 30 percent repeat/fixture/analysis allowance',
        excluded_costs=['tax','currency conversion','NAT Gateway','paid CloudWatch logs','snapshots after deletion','cross-AZ data transfer','excess EBS IOPS/throughput','download beyond account data-transfer allowance'],
        mini_result_paths=[str(p.resolve()) for p in paths])
    (out/'cost_assumptions.json').write_text(json.dumps(assumptions,indent=2),encoding='utf-8')
    text=['# EC2 예상 비용 — 본실험 미실행', '',
          '2026-11-24 확인. 서울 ap-northeast-2, Linux On-Demand, c7i.2xlarge 1대(8 vCPU/16 GiB), gp3 100 GB, public IPv4 1개를 가정한다. EC2 생성이나 유료 실험은 수행하지 않았다.', '',
          f'EC2 ${ec2_rate:.4f}/h + EBS ${100*ebs_gb_month/720:.4f}/h + IPv4 $0.0050/h = **약 ${rate:.4f}/h**. EBS 월 요금은 30일/720시간으로 비례 배분한 계산 가정이며, 볼륨을 유지하면 인스턴스 종료 후에도 스토리지 요금이 계속된다. 세금·환율·추가 전송 비용은 별도다.', '',
          '|범위|본측정 횟수|모델 시간 h|예산 시간 범위 h|예산 USD 범위|',
          '|---|---:|---:|---:|---:|']
    for r in scenarios:
        text.append(f"|{r['scenario']}|{r['measured_runs']:,}|{r['local_model_hours']:.1f}|{r['budget_low_hours']:.1f}–{r['budget_high_hours']:.1f}|${r['budget_low_usd']:.2f}–${r['budget_high_usd']:.2f}|")
    text+=['', f'EC2에서 미니 테스트만 재현하는 경우 초기 이미지 빌드·설정까지 1–2시간을 예약하면 약 **${rate:.2f}–${2*rate:.2f}**다. 이는 시간 예산이며 실제 소요 시간 보장이 아니다.', '',
       '## 계산 근거', '',
       '군집화 CPU 비용은 별도 합성 보정 데이터에서 추정한 비음수 이차식+일차식이다. 테마를 분할하지 않고 partition별 예상 비용을 합산한 뒤 Core 개수만큼 Task slot에 순서대로 배치하여 critical path를 근사했다. Spark·shuffle·반환 경로의 잔여 시간은 새 미니 테스트의 동일 Core·파티션·전략에서 관측한 값으로 보정했다. 입력량 증가에 대해 잔여 비용 고정/선형 증가의 중간값을 사용했다. AS-IS의 잔여 비용은 테마 수에 비례시켰다.', '',
       '이 값은 로컬 미니 데이터에서 미측정한 960·1,920건과 EC2 CPU로 외삽한 것이다. 0.5배/2배 시간 시나리오와 추가 30% 여유, 초기 준비 1시간을 적용했으며 통계적 신뢰구간이나 최대 청구액이 아니다. 실제 EC2에서 짧은 재검증 후 시간 예산을 다시 계산해야 한다.', '',
       'Task 효과는 Core를 고정하고 P=1/2/4/8/16을 비교하고, Core 효과는 P를 고정하고 C=1/2/4를 비교한다. 비교군은 AS-IS, CONTROL, Hash, 기사 수 LPT, 예상 연산 비용 LPT다. Driver 동적 전환 정책은 없다. AS-IS/CONTROL은 군집화 파티션 수가 없으므로 P별로 중복 반복하지 않는다.', '',
       '본실험 규모는 예산 가정이다. 각 총량에서 단일 테마 1조건 + 4/8/16/32테마 × 균등/중간/심한 편향 12조건 = 13개 데이터 조건이다. 중간/심한 편향은 최대 테마 비중 35%/65%다. 총 960·1,920건이면 모든 테마가 최소 8건을 유지하고 최대 테마도 2,000건 상한 이하다. 단일 테마를 여러 테마 수와 교차하는 불가능한 조합은 제외한다. 조건별 30회로 계산했으며 본실험은 아직 실행하지 않았다.', '',
       '## 환경 선택과 범위', '',
       'c7i.2xlarge의 8 vCPU/16 GiB에서 기존 실험 quota(Driver 1, Worker 최대 4, Master 0.5 vCPU 및 메모리 합계 약 9.5 GiB)를 유지한다. 나머지는 OS와 Docker 여유다. Spark Core는 이 환경에서 논리 vCPU 슬롯이며 물리 Core와 동일하다고 단정하지 않는다. CPU topology와 SMT 정보를 기록한다. ARM/Graviton, burstable 인스턴스, Spot은 이번 재현 환경의 CPU/아키텍처 또는 중단 변수를 추가하므로 이 견적에서 사용하지 않는다.', '',
       '한 EC2의 컨테이너 간 실험이다. 다중 물리 EC2 네트워크 분산 실험은 별도 비용과 설계가 필요하다. SSH는 본인 IP로 제한하고 Spark UI 포트는 인터넷에 공개하지 않는 구성을 가정한다. 인터넷 비용 항목에는 NAT Gateway를 포함하지 않았다. 이미지 다운로드는 측정 구간 밖이며 요금 산정의 준비 시간에 포함한다.', '',
       '## 공식 단가 출처', '',
       f"- [AWS EC2 공식 공개 단가 JSON]({prices['ec2_source']}) — 발행 {prices['ec2_manifest']['hawkFilePublicationDate']}, c7i.2xlarge rateCode {prices['ec2']['c7i.2xlarge']['rateCode']}.",
       f"- [AWS EBS 공식 공개 단가 JSON]({prices['ebs_source']}) — gp3 ${ebs_gb_month}/GB-month.",
       '- [AWS public IPv4 요금](https://aws.amazon.com/vpc/pricing/) — $0.005/IP-hour.',
       '- [AWS C7i 사양](https://aws.amazon.com/ec2/instance-types/c7i/).',
       '', '단가 원본의 선택 항목과 발행 정보는 `aws_pricing_snapshot.json`, 전체 조건과 산식 입력은 `planned_conditions.csv`, 재계산 결과는 `budget_scenarios.csv`와 `cost_assumptions.json`에 보존했다.']
    (out/'EC2_COST_ESTIMATE.md').write_text('\n'.join(text)+'\n',encoding='utf-8')
    print(json.dumps(scenarios,indent=2))


if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('directories',nargs='+',type=Path)
    p.add_argument('--output',type=Path,required=True)
    p.add_argument('--pricing',type=Path,default=Path(__file__).with_name('aws_pricing_snapshot.json'))
    a=p.parse_args();estimate(a.directories,a.output,a.pricing)
