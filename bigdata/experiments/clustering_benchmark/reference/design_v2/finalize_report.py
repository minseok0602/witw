"""Finalize the report only after all required full sessions independently pass."""
import argparse
import csv
import hashlib
import json
from pathlib import Path
from validate_results import read_csv


def main(lab, analysis):
    summary=read_csv(analysis/'summary.csv')
    rows=[r for r in summary if r['phase']=='holdout']
    assert {int(r['cores']) for r in rows}=={1,2,4}
    assert len(rows)==72 and all(int(r['n'])==30 and r['all_equal']=='True' for r in rows)
    env=json.loads((analysis/'analysis_environment.json').read_text())
    paths=[Path(p) for p in env['paths']]
    assert len(paths)==3
    statuses=[json.loads((p/'SUCCESS.json').read_text()) for p in paths]
    checks=[json.loads((p/'VALIDATED.json').read_text()) for p in paths]
    assert all(not s['quick'] and s['complete'] and s['required_repetitions']==30 for s in statuses)
    assert sum(s['measured_runs'] for s in statuses)==3780
    assert all(c['passed'] and c['oversized_timed_tasks']==0 for c in checks)
    audit=json.loads((lab/'source_audit.json').read_text())
    repo=lab.parents[2]
    untouched={name:hashlib.sha256((repo/name).read_bytes()).hexdigest()==sha for name,sha in audit['production_sha256'].items()}
    assert all(untouched.values()), 'Production source changed during experiment'
    (analysis/'PRODUCTION_UNCHANGED.json').write_text(json.dumps(untouched,indent=2))
    lookup={(int(r['cores']),r['scenario'],r['mode']):r for r in rows}
    distributed=[r for r in rows if r['mode']=='to_be_1']
    worst=min(distributed,key=lambda r:float(r['speedup_vs_control']))
    best=max(distributed,key=lambda r:float(r['speedup_vs_control']))
    slower=[r for r in distributed if float(r['speedup_vs_control'])<1]
    policy=read_csv(analysis/'policy_evaluation.csv')
    correct=sum(r['chose_fastest']=='True' for r in policy)
    text=['## 본실험 결과', '',
          '조건별 30회, 총 3,780회 성능 측정을 완료했다. 워밍업·입력 준비·probe를 제외한 수치다. 모든 후보 결과가 독립 Driver 기준과 일치했고, 각 Core의 완료·자원·Event Log 검증이 통과했다. 운영 파일 해시도 실험 시작 시점과 같다.', '',
          f"TO-BE 1은 CONTROL 대비 {len(slower)}/18개 Core·프로필 조건에서 p50이 더 길었다. 가장 불리한 조건은 {worst['cores']} Core / {worst['scenario']}로, CONTROL 대비 {(1/float(worst['speedup_vs_control'])-1)*100:.1f}% 느렸다. 가장 큰 관측 speedup은 {best['cores']} Core / {best['scenario']}의 {float(best['speedup_vs_control']):.2f}배다. 이 값들은 관측 중앙값 비교이며 통계적 유의성을 뜻하지 않는다.", '',
          '|Executor Core|프로필|AS-IS p50 s|CONTROL p50 s|TO-BE 1 p50 s|TO-BE 2 p50 s|CONTROL 기준 TO-BE 1 speedup|',
          '|---:|---|---:|---:|---:|---:|---:|']
    scenarios=['tiny','balanced','skew','single','many','large']
    for c in (1,2,4):
        for s in scenarios:
            rr=[lookup[c,s,m] for m in ['as_is','control','to_be_1','to_be_2']]
            times='|'.join(f"{float(r['p50_s']):.4f}" for r in rr)
            text.append(f"|{c}|{s}|{times}|{float(rr[2]['speedup_vs_control']):.3f}|")
    text+=['', '모든 p95·처리량·speedup·효율은 [전체 측정표](results/analysis/MEASUREMENTS.md)와 [summary.csv](results/analysis/summary.csv)에 있다. seed별 paired ratio도 별도로 제공한다.', '',
           '![처리 시간 p50과 p95](results/analysis/latency.png)', '',
           '![CONTROL 상대 speedup](results/analysis/speedup_control.png)', '',
           '## 적응형 정책의 독립 검증', '',
           f'보정 데이터로 고정한 정책은 18개 Core·프로필 중 {correct}개에서 검증 p50이 가장 짧은 고정 전략과 같은 전략을 선택했다. 10% margin 때문에 근소한 이득을 포기할 수 있으며, AS-IS를 선택 후보에서 제외한 영향도 포함한다. 이 일치 수를 통계적 분류 정확도로 일반화하지 않는다.', '',
           '|Core|프로필|선택|검증에서 가장 빠른 고정 전략|적응형 p50 / 최단 고정 p50|판단 오버헤드 us|',
           '|---:|---|---|---|---:|---:|']
    for r in policy:
        text.append(f"|{r['cores']}|{r['scenario']}|{r['chosen']}|{r['fastest_static']}|{float(r['ratio_vs_best_static']):.3f}|{float(r['route_us']):.1f}|")
    text+=['', '적응형과 선택된 고정 전략은 별도 시점에 다시 실행했으므로, 같은 실제 경로인데 적응형이 더 빠르게 관측될 수 있다. 이를 정책 자체가 실행을 가속한 효과로 해석하지 않는다. oracle 대비 비율이 1보다 작은 경우도 반복 간 변동을 포함한다.', '',
           '## Event Log와 자원 분석', '',
           '|Core|프로필|전략|Job p50|Task p50|Shuffle write MiB p50|Stage 내 max/mean 최대값 p50|Worker CPU quota 사용률 p50 %|',
           '|---:|---|---|---:|---:|---:|---:|---:|']
    for c in (1,2,4):
        for s in ('tiny','skew','single','many','large'):
            for m in ('as_is','control','to_be_1'):
                r=lookup[c,s,m]
                text.append(f"|{c}|{s}|{m}|{float(r['event_jobs_p50']):.0f}|{float(r['event_tasks_p50']):.0f}|{float(r['event_shuffle_write_bytes_p50'])/2**20:.2f}|{float(r['event_stage_max_imbalance_p50']):.2f}|{float(r['worker_quota_utilization_pct_p50']):.1f}|")
    text+=['', 'AS-IS와 CONTROL의 차이는 반복 Job/collect와 정렬 위치를 포함한 입력 경로 차이다. CONTROL과 TO-BE 1은 shuffle·Python 변환·반환 경로도 다르므로 차이를 순수 병렬화 비용으로 보지 않는다. Executor 1 Core 결과와 Core 확장 결과를 함께 확인한다. 단일 테마는 동시에 실행할 군집화 그룹이 하나뿐이며, 편향은 가장 큰 테마의 critical path가 전체 시간을 제한한다. 이 구조적 설명을 위 표의 실제 시간·Task 부하와 함께 평가해야 한다.', '',
           '![Spark Job Task Shuffle](results/analysis/spark_overhead.png)', '',
           '![Core 확장과 효율](results/analysis/strong_scaling.png)', '',
           '![CPU와 메모리](results/analysis/resources.png)', '',
           '![Task 부하 불균형](results/analysis/task_imbalance.png)', '',
           'Driver/Worker CPU·cgroup/anon 메모리, Executor JVM/Python process-tree RSS, JVM 역직렬화/결과 직렬화, Job/Stage 구간, GC/spill의 상세 수치는 summary.csv와 각 디렉터리의 enriched_runs.csv·tasks.csv·stages.csv·executor_metrics.csv에서 확인할 수 있다. RSS는 process-tree 합계로 공유 페이지가 중복될 수 있다. 아래의 측정 범위와 한계를 함께 적용한다.', '']
    report=lab/'REPORT.md'
    body=report.read_text(encoding='utf-8')
    marker='## 검증한 범위'
    body=body[body.index(marker):]
    body=body.replace('본실험 원시 자료는 `results/full-{1,2,4}-main-v2/`에 생성한다. 최종 통합 집계와 그래프는 본실험 완료 후 `results/analysis/`에 만든다.',
                      '본실험 원시 자료는 `results/full-{1,2,4}-main-v2/`, 최종 통합 집계와 그래프는 `results/analysis/`에 보존했다.')
    report.write_text('# HDBSCAN 실행 전략 비교 실험\n\n'+'\n'.join(text)+'\n'+body,encoding='utf-8')
    print(f'Finalized {report}')


if __name__=='__main__':
    p=argparse.ArgumentParser()
    p.add_argument('--analysis',type=Path,required=True)
    a=p.parse_args()
    main(Path(__file__).resolve().parent,a.analysis)
