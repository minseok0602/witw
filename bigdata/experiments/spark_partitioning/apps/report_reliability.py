"""Analyze completed EC2 sessions only. Refuses partial/mixed/duplicate 540-run results."""
import argparse
import csv
import hashlib
import json
import shutil
import statistics
from pathlib import Path
import numpy as np
import analyze
from event_metrics import write_csv


def read(path):
    with path.open(encoding='utf-8-sig',newline='') as f:return list(csv.DictReader(f))


def main():
    parser=argparse.ArgumentParser();parser.add_argument('raw',type=Path);parser.add_argument('--output',type=Path,required=True)
    args=parser.parse_args();paths=[args.raw/f'{app}-full-session{session}' for session in [1,2] for app in ['A','B']]
    for path in paths:
        valid=json.loads((path/'VALIDATED.json').read_text());env=json.loads((path/'environment.json').read_text())
        assert valid['passed'] and valid['measured_runs']==135
        assert env['mode']=='full' and env['reps']==15 and env['session'] in [1,2]
    assert len({json.loads((p/'environment.json').read_text())['benchmark_sha256'] for p in paths})==1
    analyze.main(paths,args.output,True)
    rows=read(args.output/'all_runs.csv');summary=read(args.output/'summary.csv')
    assert len(rows)==540 and len(summary)==18 and all(int(r['n'])==30 for r in summary)
    assert json.loads((args.output/'analysis_manifest.json').read_text())['performance_conclusions_allowed']
    for app in ['A','B']:
        for condition in ['NATURAL','MODERATE','SKEW']:
            for strategy in ([app+'0',app+'1',app+'2']):
                group=[r for r in rows if (r['app'],r['scenario'],r['mode'])==(app,condition,strategy)]
                assert {int(r['rep']) for r in group}==set(range(30))
    incremental=[]
    for app in ['A','B']:
        for condition in ['NATURAL','MODERATE','SKEW']:
            cheap={(r['seed'],r['rep']):r for r in rows if (r['app'],r['scenario'],r['mode'])==(app,condition,app+'1')}
            expensive=[r for r in rows if (r['app'],r['scenario'],r['mode'])==(app,condition,app+'2')]
            differences=[float(cheap[(r['seed'],r['rep'])]['wall_s'])-float(r['wall_s']) for r in expensive]
            rng=np.random.default_rng(731);values=np.array(differences)
            boot=np.mean(values[rng.integers(0,len(values),size=(10000,len(values)))],axis=1)
            lo,hi=np.percentile(boot,[2.5,97.5])
            incremental.append(dict(app=app,scenario=condition,comparison=app+'2 vs '+app+'1',n=30,
                paired_mean_saved_s=statistics.mean(differences),paired_ci95_low_s=float(lo),paired_ci95_high_s=float(hi)))
    write_csv(args.output/'incremental_cost_aware.csv',incremental)
    sessions=[]
    for path in paths:
        env=json.loads((path/'environment.json').read_text())
        for condition in env['scenarios']:
            for strategy in [env['app']+str(i) for i in range(3)]:
                rr=[r for r in read(path/'runs.csv') if r['phase']=='full' and r['scenario']==condition and r['mode']==strategy]
                sessions.append(dict(session=env['session'],app=env['app'],condition=condition,strategy=strategy,n=len(rr),
                                     p50_s=statistics.median(float(r['wall_s']) for r in rr)))
    write_csv(args.output/'per_session.csv',sessions)
    model=json.loads((args.raw/'B-quick-session1/cost_model.json').read_text())
    lines=['# EC2 A/B 파티셔닝 540회 실험 보고서','',
        '두 세션의 각15개 독립 입력 블록을 합쳐 조건별30회,총540회를 실제 실행했다. 본 보고서는 전체 검증을 통과한 원시 데이터에서만 생성한다. 운영 코드와 운영 데이터는 변경하지 않았다. 통합 C 및 Kafka/DB/HDFS/LLM 비용은 포함하지 않았다.', '',
        '## 환경과 입력','',
        '서울의 같은 AZ에 Head m7i.xlarge1대,Worker m7i.large2대를 사용했다. 두 Executor에 각각2 Spark Core,총4개 논리적 실행 슬롯과4파티션을 고정했다. M7i의 vCPU와 물리 Core를 동일시하지 않으며 실제 lscpu 정보는 node JSON에 보존한다. Driver container CPU1/3GiB/heap1536MiB,Worker 각각 CPU2/4GiB/executor heap2GiB다.', '',
        'A는4000건,테마별500건,후보200/100/50/10/150/80/30/20개다. B는2000건,테마별450/400/350/300/150/150/100/100개다. 같은 반복의 세 조건에서 논리적 기사·임베딩·총 연산량을 유지했다. A는 기사 ID에 입력 seed를 포함해 자연 Hash 배치도 반복마다 변하게 했다. B는 논리적 기사를 유지하고 테마 ID alias로 Hash 충돌을 조절했다.', '',
        '|조건|A의 초기 부하|B0의 테마 충돌|','|---|---|---|',
        '|NATURAL|기사 ID Hash,강제 배치 없음|연속8개 테마 ID에 비용 구성을 시드로 대응|',
        '|MODERATE|각1000건,후보 비교137000/86000/56000/41000|기사850/650/300/200 형태|',
        '|SKEW|각1000건,후보 비교175000/90000/40000/15000|기사1500/200/150/150 형태,큰 테마4개 충돌|','',
        '두 세션 사이 인스턴스를 정지·재시작했다. 입력 블록과 전략 순서를 무작위 교차하고 JVM/Python 워밍업은 통계에서 제외했다. B 비용 모델은 첫 EC2 Quick의 독립 Executor 보정에서 선택한 모델을 두 세션에 동결해 사용했다.', '',
        '## 전체 성능','',
        '단위는 초이며 각 셀n=30이다. 시간에는 추가 집계·배치 계획·Shuffle·원본 연산·최종 collect를 포함했다. 원본 참조 실행·공통 입력 준비·CSV 저장은 주timer 밖이다. 아래 감소율은 각 조건의 A0/B0 대비이며 음수는 느려졌음을 뜻한다.', '',
        '|App|조건|전략|p50 s|p95 s|감소율 %|Task CV|paired 절약시간 95% 구간 s|',
        '|---|---|---|---:|---:|---:|---:|---|']
    for r in summary:
        lines.append(f"|{r['app']}|{r['scenario']}|{r['strategy']}|{float(r['p50_s']):.3f}|{float(r['p95_s']):.3f}|{float(r['reduction_pct']):.1f}|{float(r['task_cv_p50']):.3f}|[{float(r['paired_ci95_low_s']):.3f}, {float(r['paired_ci95_high_s']):.3f}]|")
    lines+=['','![A 처리 시간과 Task 불균형](A-latency-imbalance.png)','',
            '![B 처리 시간과 Task 불균형](B-latency-imbalance.png)','',
            '## 비용 모델 추가 효과','',
            '기사 수 기반 전략보다 비용 기반 전략이 얼마나 더 유리한지 같은 입력끼리 비교했다. 양수는 비용 기반이 빠른 것이다. 기준군 대비 개선과 이 추가 효과를 혼동하지 않는다.', '',
            '|App|조건|비교|평균 절약 s|paired 95% 구간 s|','|---|---|---|---:|---|']
    for r in incremental:
        lines.append(f"|{r['app']}|{r['scenario']}|{r['comparison']}|{r['paired_mean_saved_s']:.4f}|[{r['paired_ci95_low_s']:.4f}, {r['paired_ci95_high_s']:.4f}]|")
    lines+=['',f"동결한 HDBSCAN 모델은 `{model['selected']}`다. 독립 모델 검증의 크기별 중앙값에 대한 MAE는 "+', '.join(f"{k}={v['mae_s']:.4f}s" for k,v in model['errors'].items())+'다. 이 모델은 합성 밀도·CPU·동시 실행 조건에 의존하며 HDBSCAN 복잡도에 대한 증명이 아니다.', '',
        '## 정합성·오버헤드·한계','',
        '각 비교군은 동일 입력의 원본 함수 참조와 결과를 비교했다. A는 기사ID·할당 이슈·미분류·최고 유사도·누락/중복을 검증했다. B는 입력 목록·군집 멤버 집합·미분류·대표 기사·centroid·confidence를 검증했다. 동일 테마 분할이 없고 계획한 파티션과 실제 TaskContext가 일치했다. 실제 ExecutorAdded=[2,2]와 연산 Stage4Task도 검사했다.', '',
        'Event Log의 Job/Stage/Task·Shuffle·fetch wait·JVM de/serialize·GC·spill,Python kernel CPU/시간과250ms cgroup CPU/메모리/NIC를 원시에 저장했다. 조건별 상세 값은summary.csv/all_runs.csv,세션별 재현성은per_session.csv에 있다. 병렬 Task 시간 합을wall에서 빼지 않았다. JVM CPU/직렬화 지표가 Python 전체 비용은 아니다. Host network 모드의 NIC는 해당 서버 전체 전송량이며 프로세스별 전송량은 아니다.', '',
        '이 실험은 합성 입력에서의 연산별 비교다. 자연 Hash 조건도 실제 운영 배치의 관측 빈도를 추정한 것은 아니다. 운영 Kafka6파티션/murmur2와 정확히 같지 않은4파티션 기사ID xxhash64를 사용했다. 두 세션·30블록으로 모든 하드웨어 잡음·테마 밀도·데이터 규모를 대표할 수 없다. 운영 성능 개선률과 전체 파이프라인 효과를 직접 주장하지 않는다.', '',
        'B의 큰 테마 하나는 분할하지 않으므로 하나의 테마가 지배하는 상황을 재배치만으로 해결할 수 없다. 이번 SKEW는 큰 테마 여러 개가 같은 Task에 몰리는 경우다. A1은 기사 수를 맞추면서 기사 구성을 다시 섞어 비용 편향도 줄일 수 있으므로 A2의 추가 비용 추정이 필수라고 가정하지 않는다.', '',
        '## 재현 자료','',
        '- [요약 CSV](summary.csv),[paired 차이](paired.csv),[추가 비용 모델 비교](incremental_cost_aware.csv),[세션별 결과](per_session.csv).',
        '- 원시 디렉터리의runs.csv/partition_runs.csv/plans.json/inputs.csv/environment.json/VALIDATED.json과spark-events를 보존했다.',
        '- 환경·소스 SHA와 라이브러리 버전,실제 Docker 제한·이미지ID는 각 실행 및 인접containers.json에 기록했다.',
        '- AWS 가동 시간과 회수·삭제 영수증에 따른 비용 정산은 별도 운영 기록을 사용한다. 본 보고서 생성은 추가 AWS 자원을 만들지 않는다.','']
    (args.output/'REPORT.md').write_text('\n'.join(lines),encoding='utf-8')
    code=args.output/'analysis_code';code.mkdir(exist_ok=True)
    for filename in ['report_reliability.py','analyze.py','event_metrics.py','validate.py']:
        shutil.copy2(Path(__file__).with_name(filename),code/filename)
    print(args.output/'REPORT.md')


if __name__=='__main__':main()
