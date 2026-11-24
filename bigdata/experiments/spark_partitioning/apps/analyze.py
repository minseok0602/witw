import argparse
import collections
import csv
import json
import statistics
from pathlib import Path
import numpy as np
from event_metrics import write_csv
from validate import read,validate


def interval_union(pairs):
    total=0.;end=None
    for a,b in sorted(pairs):
        total+=b-a if end is None or a>end else max(0,b-end)
        end=max(end or b,b)
    return total


def enrich(root):
    if not (root/'VALIDATED.json').exists():validate(root)
    env=json.loads((root/'environment.json').read_text())
    events={r['group']:r for r in read(root/'event_metrics.csv')}
    tasks=read(root/'tasks.csv');stages=read(root/'stages.csv');parts=read(root/'partition_runs.csv')
    resources={role:read(root/f'resources-{role}.csv') for role in ['driver','worker-1','worker-2']}
    arrays={role:{k:np.array([float(r[k]) for r in rows]) for k in rows[0]} for role,rows in resources.items()}
    enriched=[]
    for raw in read(root/'runs.csv'):
        if raw['phase']=='warmup':continue
        r=dict(raw)
        for k in ('wall_s','plan_s','extra_aggregate_s','kernel_sum_s','kernel_cpu_s','epoch_start','epoch_end'):r[k]=float(r[k])
        r['n']=int(r['n']);r['rep']=int(r['rep']);r['seed']=int(r['seed'])
        for k,v in events[r['group']].items():
            if k!='group':r['event_'+k]=float(v) if v else 0.
        pp=[p for p in parts if p['group']==r['group']]
        tt=[t for t in tasks if t['group']==r['group'] and t['stage_id']==r['cluster_stage_id']]
        values=[float(t['duration_ms']) for t in tt]
        r['target_tasks']=len(tt);r['empty_tasks']=sum(p['empty']=='True' for p in pp)
        r['task_mean_ms']=statistics.mean(values) if values else 0
        r['task_std_ms']=statistics.pstdev(values) if values else 0
        r['task_max_ms']=max(values,default=0.)
        r['task_cv']=r['task_std_ms']/r['task_mean_ms'] if r['task_mean_ms'] else 0
        ss=[s for s in stages if s['group']==r['group']]
        r['stage_count']=len(ss)
        pairs=[(float(s['submission_ms']),float(s['completion_ms'])) for s in ss if s['submission_ms'] and s['completion_ms']]
        r['stage_union_ms']=interval_union(pairs)
        target=[s for s in ss if s['stage_id']==r['cluster_stage_id']]
        r['target_stage_ms']=float(target[0]['duration_ms']) if target else 0
        r['target_task_queue_wait_sum_ms']=sum(max(0,float(t['launch_ms'])-float(target[0]['submission_ms'])) for t in tt) if target else 0
        for role,a in arrays.items():
            start,end=r['epoch_start'],r['epoch_end'];mask=(a['epoch_s']>=start)&(a['epoch_s']<=end)
            idx=np.where(mask)[0].tolist()+[max(0,int(np.searchsorted(a['epoch_s'],start))-1),min(len(a['epoch_s'])-1,int(np.searchsorted(a['epoch_s'],end)))]
            for k in ['cpu_usage_s','net_rx_bytes','net_tx_bytes','oom_events','oom_kills']:
                if k in a:r[role+'_'+k]=float(np.interp(end,a['epoch_s'],a[k])-np.interp(start,a['epoch_s'],a[k]))
            for k in ['memory_bytes','anon_bytes']:
                r[role+'_'+k+'_peak']=float(max(a[k][idx]))
        r['worker_cpu_s']=sum(r[k+'_cpu_usage_s'] for k in ['worker-1','worker-2'])
        r['worker_cpu_util_pct']=100*r['worker_cpu_s']/(4*r['wall_s'])
        r['worker_idle_capacity_s_estimate']=max(0,4*r['wall_s']-r['worker_cpu_s'])
        enriched.append(r)
    write_csv(root/'enriched_runs.csv',enriched)
    return env,enriched


def main(paths,out,plots=False):
    out.mkdir(parents=True,exist_ok=True)
    rows=[];envs=[]
    for path in paths:
        env,records=enrich(path);envs.append(env);rows+=records
    assert len({e['mode'] for e in envs})==1,'Analyze quick/explore/full separately'
    mode=envs[0]['mode']
    assert len({e['driver']['cluster_sha256'] for e in envs})==1
    assert len({e['driver']['match_sha256'] for e in envs})==1
    unique={(r['app'],r['scenario'],r['mode'],r['seed'],r['rep']) for r in rows}
    assert len(unique)==len(rows),'Duplicate input block/strategy measurements across result directories'
    models=[(path/'cost_model.json').read_text() for path,e in zip(paths,envs) if e['app']=='B']
    assert len(set(models))<=1,'B cost model changed between measurement sessions'
    grouped=collections.defaultdict(list)
    for r in rows:grouped[(r['app'],r['scenario'],r['mode'])].append(r)
    summary=[];paired=[];selection=[]
    for (app,scenario,strategy),rr in sorted(grouped.items()):
        baseline='A0' if app=='A' else 'B0'
        base=grouped[(app,scenario,baseline)]
        by_pair={(r['seed'],r['rep']):r for r in base}
        med=statistics.median(r['wall_s'] for r in rr);bmed=statistics.median(r['wall_s'] for r in base)
        diffs=[];ratios=[]
        for r in rr:
            b=by_pair[(r['seed'],r['rep'])]
            assert r['input_sha256']==b['input_sha256']
            difference=b['wall_s']-r['wall_s'];diffs.append(difference);ratios.append(b['wall_s']/r['wall_s'])
            paired.append(dict(app=app,scenario=scenario,strategy=strategy,seed=r['seed'],rep=r['rep'],
                baseline_wall_s=b['wall_s'],strategy_wall_s=r['wall_s'],difference_s=difference,speedup=b['wall_s']/r['wall_s']))
        samples=np.array(diffs);rng=np.random.default_rng(731)
        boot=np.mean(samples[rng.integers(0,len(samples),size=(10000,len(samples)))],axis=1)
        ci=np.percentile(boot,[2.5,97.5]) if len(samples)>1 else [None,None]
        result=dict(app=app,scenario=scenario,strategy=strategy,n=len(rr),mean_s=statistics.mean(r['wall_s'] for r in rr),
            p50_s=med,p95_s=float(np.percentile([r['wall_s'] for r in rr],95)),std_s=statistics.pstdev(r['wall_s'] for r in rr),
            articles_per_s=rr[0]['n']/med,speedup=bmed/med,reduction_pct=100*(1-med/bmed),throughput_increase_pct=100*(bmed/med-1),
            paired_mean_difference_s=statistics.mean(diffs),paired_ci95_low_s=float(ci[0]) if ci[0] is not None else None,paired_ci95_high_s=float(ci[1]) if ci[1] is not None else None,
            confidence_note='bootstrap percentile over paired input blocks; exploratory small n is not confirmatory',
            all_equal=all(r['equal']=='True' for r in rr))
        metrics=['plan_s','extra_aggregate_s','kernel_sum_s','kernel_cpu_s','task_mean_ms','task_std_ms','task_max_ms','task_cv',
                 'target_tasks','empty_tasks','target_stage_ms','stage_count','stage_union_ms','target_task_queue_wait_sum_ms',
                 'worker_cpu_util_pct','worker_idle_capacity_s_estimate']
        metrics+=[k for k in rr[0] if k.startswith(('event_','driver_','worker-1_','worker-2_')) and isinstance(rr[0][k],(float,int))]
        for k in metrics:result[k+'_p50']=statistics.median(r[k] for r in rr)
        summary.append(result)
    if mode=='full':
        for app in ['A','B']:
            for scenario in sorted({r['scenario'] for r in summary if r['app']==app}):
                choices=[r for r in summary if r['app']==app and r['scenario']==scenario and r['strategy'] in (['A1','A2'] if app=='A' else ['B1','B2'])
                    and r['n']>=30 and r['reduction_pct']>=10 and r['paired_ci95_low_s']>0]
                best=min(choices,key=lambda r:r['p50_s']) if choices else None
                selection.append(dict(app=app,scenario=scenario,selected=best['strategy'] if best else ('A0' if app=='A' else 'B0'),
                    rule='>=10% p50 improvement and positive paired 95% interval; otherwise preserve baseline'))
        (out/'strategy_selection.json').write_text(json.dumps(selection,indent=2))
    write_csv(out/'summary.csv',summary);write_csv(out/'paired.csv',paired);write_csv(out/'all_runs.csv',rows)
    (out/'analysis_manifest.json').write_text(json.dumps(dict(mode=mode,inputs=[str(p.resolve()) for p in paths],
        rows=len(rows),performance_conclusions_allowed=mode=='full' and all(r['n']>=30 for r in summary)),indent=2))
    lines=['# '+mode+' measurements','', '|App|Distribution|Strategy|n|p50 s|p95 s|CV Task p50|Speedup|',
        '|---|---|---|---:|---:|---:|---:|---:|']
    for r in summary:
        cv='N/A' if r['strategy'] in ['AS_IS','CONTROL'] else f"{r['task_cv_p50']:.3f}"
        lines.append(f"|{r['app']}|{r['scenario']}|{r['strategy']}|{r['n']}|{r['p50_s']:.4f}|{r['p95_s']:.4f}|{cv}|{r['speedup']:.3f}|")
    lines+=['','Exploratory/quick values do not establish a winning strategy. Task sums and JVM metric sums are not elapsed overhead. Worker idle capacity is inferred from 250ms cgroup samples. Local container network counters are not measurements of inter-server EC2 traffic.']
    (out/'MEASUREMENTS.md').write_text('\n'.join(lines)+'\n',encoding='utf-8')
    if plots:
        import matplotlib;matplotlib.use('Agg')
        import matplotlib.pyplot as plt
        for app in sorted({r['app'] for r in summary}):
            subset=[r for r in summary if r['app']==app]
            strategies=sorted({r['strategy'] for r in subset});fig,axes=plt.subplots(1,2,figsize=(12,4))
            for color_index,strategy in enumerate(strategies):
                color=f'C{color_index}'
                rr=sorted([r for r in subset if r['strategy']==strategy],key=lambda r:r['scenario'])
                axes[0].plot([r['scenario'] for r in rr],[r['p50_s'] for r in rr],marker='o',label=strategy,color=color)
                if strategy not in ['AS_IS','CONTROL']:
                    axes[1].plot([r['scenario'] for r in rr],[r['task_cv_p50'] for r in rr],marker='o',label=strategy,color=color)
            axes[0].set(ylabel='p50 wall seconds',title=f'App {app}: {mode}, no confirmatory conclusion' if mode!='full' else f'App {app}')
            axes[1].set(ylabel='Target-stage task CV',title='Task imbalance (includes empty tasks)')
            for ax in axes:
                ax.legend();ax.spines[['top','right']].set_visible(False);ax.set_ylim(bottom=0)
            fig.tight_layout();fig.savefig(out/f'{app}-latency-imbalance.png',dpi=160);plt.close(fig)
    print(f'Analyzed {len(rows)} actual measured runs in {out}')


if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('directories',nargs='+',type=Path);p.add_argument('--output',required=True,type=Path)
    p.add_argument('--plots',action='store_true');a=p.parse_args();main(a.directories,a.output,a.plots)
