"""Reproducible aggregation and static Matplotlib figures; no invented missing measurements."""
import argparse
import collections
import csv
import json
import statistics
from pathlib import Path
import numpy as np
from event_metrics import write_csv
from validate_results import read_csv, validate

MODES=['as_is','control','to_be_1','to_be_2']
LABELS=['AS-IS','CONTROL','TO-BE 1','TO-BE 2']
SCENARIOS=['tiny','balanced','skew','single','many','large']


def enrich(root):
    if not (root/'VALIDATED.json').exists():
        validate(root)
    events={r['group']:r for r in read_csv(root/'event_metrics.csv')}
    jobs=read_csv(root/'jobs.csv')
    stages=read_csv(root/'stages.csv')
    executor_peaks=read_csv(root/'executor_metrics.csv') if (root/'executor_metrics.csv').exists() else []
    def union_ms(intervals):
        end,total=None,0.
        for start,finish in sorted(intervals):
            if end is None or start>end:
                total+=finish-start
            elif finish>end:
                total+=finish-end
            end=max(end or finish,finish)
        return total
    resources={}
    for role in ('driver','worker'):
        samples=read_csv(root/f'resources-{role}.csv')
        resources[role]={k:np.array([float(r[k]) for r in samples]) for k in samples[0]}
    records=[]
    for raw in read_csv(root/'runs.csv'):
        if raw['phase'] not in ('calibration','holdout'):
            continue
        r=dict(raw)
        for key in ('wall_s','route_s','kernel_sum_s','kernel_max_s','kernel_cpu_s','articles_per_s','epoch_start','epoch_end'):
            r[key]=float(r[key])
        for key in ('cores','selected_articles','input_articles','rep','seed'):
            r[key]=int(r[key])
        for k,v in events[r['group']].items():
            if k!='group':
                r['event_'+k]=float(v) if v else 0.
        jj=[j for j in jobs if j['group']==r['group'] and j['completion_ms']]
        ss=[j for j in stages if j['group']==r['group'] and j['completion_ms'] and j['submission_ms']]
        r['event_job_union_ms']=union_ms([(float(j['submission_ms']),float(j['completion_ms'])) for j in jj])
        r['event_stage_union_ms']=union_ms([(float(j['submission_ms']),float(j['completion_ms'])) for j in ss])
        r['event_stage_max_imbalance']=max((float(j['task_max_mean_ratio']) for j in ss),default=0)
        peaks=[j for j in executor_peaks if j['group']==r['group']]
        for key,name in [('ProcessTreeJVMRSSMemory','jvm_rss'),('ProcessTreePythonRSSMemory','python_rss'),('JVMHeapMemory','jvm_heap')]:
            r['event_executor_'+name+'_peak_mib']=max((float(j[key])/2**20 for j in peaks),default=0)
        for role,s in resources.items():
            start,end=r['epoch_start'],r['epoch_end']
            delta=float(np.interp(end,s['epoch_s'],s['cpu_usage_s'])-np.interp(start,s['epoch_s'],s['cpu_usage_s']))
            throttle=float(np.interp(end,s['epoch_s'],s['throttled_s'])-np.interp(start,s['epoch_s'],s['throttled_s']))
            mask=(s['epoch_s']>=start)&(s['epoch_s']<=end)
            indexes=np.where(mask)[0].tolist()
            indexes+= [max(0,int(np.searchsorted(s['epoch_s'],start))-1),min(len(s['epoch_s'])-1,int(np.searchsorted(s['epoch_s'],end)))]
            r[role+'_cpu_s']=delta
            r[role+'_cpu_cores']=delta/r['wall_s']
            r[role+'_quota_utilization_pct']=100*delta/r['wall_s']/(r['cores'] if role=='worker' else 1)
            r[role+'_memory_peak_mib']=float(max(s['memory_bytes'][indexes])/2**20)
            r[role+'_anon_peak_mib']=float(max(s['anon_bytes'][indexes])/2**20)
            r[role+'_throttled_s']=throttle
        records.append(r)
    write_csv(root/'enriched_runs.csv',records)
    return records


def analyze(paths,out):
    out.mkdir(parents=True,exist_ok=True)
    environments=[json.loads((p/'environment.json').read_text()) for p in paths]
    assert len({e['quick'] for e in environments})==1, 'Do not mix Quick Test with full measurements'
    assert len({e['cores'] for e in environments})==len(paths), 'One completed session per core count required'
    assert all(e['driver']==environments[0]['driver'] for e in environments), 'Library/source mismatch across sessions'
    assert len({e.get('input_backend','legacy') for e in environments})==1
    records=[]
    for path in paths:
        records+=enrich(path)
    assert len({json.loads((p/'environment.json').read_text())['benchmark_sha256'] for p in paths})==1
    by_input=collections.defaultdict(set)
    for r in records:
        by_input[(r['phase'],r['scenario'],r['seed'])].add(r['input_digest'])
    assert all(len(v)==1 for v in by_input.values()), 'Input differs across cores or modes'
    conditions=collections.defaultdict(list)
    for r in records:
        conditions[(r['cores'],r['phase'],r['scenario'],r['mode'])].append(r)
    summary=[]
    for (cores,phase,scenario,mode),rr in sorted(conditions.items()):
        wall=[r['wall_s'] for r in rr]
        same=lambda m:conditions[(cores,phase,scenario,m)]
        base=statistics.median(r['wall_s'] for r in same('as_is'))
        control=statistics.median(r['wall_s'] for r in same('control'))
        med=statistics.median(wall)
        row=dict(cores=cores,phase=phase,scenario=scenario,mode=mode,n=len(rr),selected_articles=rr[0]['selected_articles'],
                 p50_s=med,p95_s=float(np.percentile(wall,95)),articles_per_s=rr[0]['selected_articles']/med,
                 speedup_vs_as_is=base/med,speedup_vs_control=control/med,
                 control_relative_efficiency=(control/med)/cores,
                 actual_modes=';'.join(sorted({r['actual'] for r in rr})),all_equal=all(r['equal']=='True' for r in rr))
        for k in rr[0]:
            if k.startswith(('event_','driver_','worker_')) or k in ('kernel_sum_s','kernel_max_s','kernel_cpu_s','route_s'):
                if isinstance(rr[0][k],(float,int)):
                    row[k+'_p50']=statistics.median(r[k] for r in rr)
        summary.append(row)
    for row in summary:
        key=(1,row['phase'],row['scenario'],row['mode'])
        if key in conditions:
            t1=statistics.median(r['wall_s'] for r in conditions[key])
            row['strong_scaling_speedup']=t1/row['p50_s']
            row['strong_scaling_efficiency']=row['strong_scaling_speedup']/row['cores']
        else:
            row['strong_scaling_speedup']=None
            row['strong_scaling_efficiency']=None
    write_csv(out/'summary.csv',summary)
    write_csv(out/'all_runs.csv',records)
    paired=[]
    for cores in sorted({r['cores'] for r in records}):
        for phase in ('calibration','holdout'):
            for scenario in SCENARIOS:
                rr=[r for r in records if (r['cores'],r['phase'],r['scenario'])==(cores,phase,scenario)]
                pairs=collections.defaultdict(dict)
                for r in rr:
                    pairs[(r['seed'],r['rep'])][r['mode']]=r
                for (seed,rep),by_mode in pairs.items():
                    best=min(MODES[:3],key=lambda m:by_mode[m]['wall_s'])
                    for mode,r in by_mode.items():
                        paired.append(dict(cores=cores,phase=phase,scenario=scenario,seed=seed,rep=rep,mode=mode,
                            actual=r['actual'],speedup_vs_as_is=by_mode['as_is']['wall_s']/r['wall_s'],
                            speedup_vs_control=by_mode['control']['wall_s']/r['wall_s'],
                            observed_best=best,observed_regret_ratio=r['wall_s']/by_mode[best]['wall_s']-1))
    write_csv(out/'paired_ratios.csv',paired)
    seed_rows=[]
    for cores,phase,scenario,mode in sorted(conditions):
        for seed in sorted({r['seed'] for r in conditions[(cores,phase,scenario,mode)]}):
            rr=[r for r in paired if (r['cores'],r['phase'],r['scenario'],r['mode'],r['seed'])==(cores,phase,scenario,mode,seed)]
            seed_rows.append(dict(cores=cores,phase=phase,scenario=scenario,mode=mode,seed=seed,n=len(rr),
                paired_speedup_vs_control_p50=statistics.median(r['speedup_vs_control'] for r in rr)))
    write_csv(out/'seed_ratios.csv',seed_rows)
    policy=[]
    lookup={(r['cores'],r['phase'],r['scenario'],r['mode']):r for r in summary}
    for cores in sorted({r['cores'] for r in records}):
        for scenario in SCENARIOS:
            static=[lookup[(cores,'holdout',scenario,m)] for m in MODES[:3]]
            best=min(static,key=lambda r:r['p50_s'])
            adaptive=lookup[(cores,'holdout',scenario,'to_be_2')]
            chosen=adaptive['actual_modes']
            policy.append(dict(cores=cores,scenario=scenario,chosen=chosen,fastest_static=best['mode'],
                chose_fastest=chosen==best['mode'],adaptive_p50_s=adaptive['p50_s'],adaptive_p95_s=adaptive['p95_s'],
                ratio_vs_best_static=adaptive['p50_s']/best['p50_s'],speedup_vs_control=adaptive['speedup_vs_control'],
                route_us=adaptive['route_s_p50']*1e6))
    write_csv(out/'policy_evaluation.csv',policy)
    lines=['# Measured results','', '|Cores|Profile|Mode|n|p50 (s)|p95 (s)|articles/s|AS-IS speedup|CONTROL speedup|',
           '|---:|---|---|---:|---:|---:|---:|---:|---:|']
    for r in summary:
        if r['phase']=='holdout':
            lines.append(f"|{r['cores']}|{r['scenario']}|{r['mode']}|{r['n']}|{r['p50_s']:.4f}|{r['p95_s']:.4f}|{r['articles_per_s']:.1f}|{r['speedup_vs_as_is']:.3f}|{r['speedup_vs_control']:.3f}|")
    lines+=['','## Adaptive policy','', '|Cores|Profile|Chosen|Fastest static|Adaptive / fastest static|Route us|',
            '|---:|---|---|---|---:|---:|']
    for r in policy:
        lines.append(f"|{r['cores']}|{r['scenario']}|{r['chosen']}|{r['fastest_static']}|{r['ratio_vs_best_static']:.3f}|{r['route_us']:.1f}|")
    (out/'MEASUREMENTS.md').write_text('\n'.join(lines)+'\n',encoding='utf-8')
    (out/'analysis_environment.json').write_text(json.dumps(dict(numpy=np.__version__,paths=[str(p.resolve()) for p in paths]),indent=2))
    return summary,records


def plots(summary,records,out):
    import matplotlib
    matplotlib.use('Agg')
    import matplotlib.pyplot as plt
    plt.rcParams.update({'font.size':10,'axes.spines.top':False,'axes.spines.right':False})
    colors=['#64748b','#0891b2','#ea580c','#7c3aed']
    cores=sorted({r['cores'] for r in summary})
    lookup={(r['cores'],r['scenario'],r['mode']):r for r in summary if r['phase']=='holdout'}
    fig,axes=plt.subplots(len(cores),1,figsize=(13,4*len(cores)),squeeze=False)
    for ax,c in zip(axes.flat,cores):
        x=np.arange(len(SCENARIOS))
        for i,m in enumerate(MODES):
            rr=[lookup[c,s,m] for s in SCENARIOS]
            xx=x+(i-1.5)*.19
            ax.bar(xx,[r['p50_s'] for r in rr],width=.18,color=colors[i],label=LABELS[i])
            ax.scatter(xx,[r['p95_s'] for r in rr],s=16,c='black',marker='_',zorder=5)
        ax.set(xticks=x,xticklabels=SCENARIOS,yscale='log',ylabel='Elapsed seconds (log)',title=f'{c} executor cores: p50 bars, p95 black ticks')
        ax.legend(ncol=4)
    fig.tight_layout();fig.savefig(out/'latency.png',dpi=160);plt.close(fig)
    fig,axes=plt.subplots(1,len(cores),figsize=(5*len(cores),4),squeeze=False)
    for ax,c in zip(axes.flat,cores):
        for i,m in enumerate(MODES[1:],1):
            ax.plot(SCENARIOS,[lookup[c,s,m]['speedup_vs_control'] for s in SCENARIOS],marker='o',color=colors[i],label=LABELS[i])
        ax.axhline(1,color='gray',ls='--');ax.set(title=f'{c} executor cores',ylabel='CONTROL p50 / strategy p50');ax.tick_params(axis='x',rotation=30);ax.legend()
    fig.tight_layout();fig.savefig(out/'speedup_control.png',dpi=160);plt.close(fig)
    if len(cores)>1:
        fig,axes=plt.subplots(1,2,figsize=(12,4))
        for s in SCENARIOS:
            rr=[lookup[c,s,'to_be_1'] for c in cores]
            axes[0].plot(cores,[r['strong_scaling_speedup'] for r in rr],marker='o',label=s)
            axes[1].plot(cores,[r['strong_scaling_efficiency'] for r in rr],marker='o',label=s)
        axes[0].plot(cores,cores,'k--',label='ideal');axes[1].axhline(1,color='gray',ls='--')
        axes[0].set(xlabel='Executor cores',ylabel='TO-BE 1 t(1) / t(p)');axes[1].set(xlabel='Executor cores',ylabel='Strong-scaling efficiency')
        axes[0].legend(ncol=2);fig.tight_layout();fig.savefig(out/'strong_scaling.png',dpi=160);plt.close(fig)
    fig,axes=plt.subplots(len(cores),3,figsize=(15,3.8*len(cores)),squeeze=False)
    for row,c in zip(axes,cores):
        for i,m in enumerate(MODES[:3]):
            rr=[lookup[c,s,m] for s in SCENARIOS]
            for ax,field,label,scale in zip(row,['event_jobs_p50','event_tasks_p50','event_shuffle_write_bytes_p50'],
                ['Spark jobs','Spark tasks','Shuffle write (MiB)'],[1,1,2**20]):
                ax.plot(SCENARIOS,[r[field]/scale for r in rr],marker='o',label=LABELS[i],c=colors[i])
                ax.set(title=f'{c} executor cores',ylabel=label);ax.tick_params(axis='x',rotation=30)
        row[0].legend()
    fig.tight_layout();fig.savefig(out/'spark_overhead.png',dpi=160);plt.close(fig)
    fig,axes=plt.subplots(len(cores),4,figsize=(19,3.5*len(cores)),squeeze=False)
    fields=['driver_quota_utilization_pct_p50','worker_quota_utilization_pct_p50','driver_memory_peak_mib_p50','worker_memory_peak_mib_p50']
    labels=['Driver CPU quota used (%)','Worker CPU quota used (%)','Driver cgroup memory (MiB)','Worker cgroup memory (MiB)']
    for row,c in zip(axes,cores):
        for i,m in enumerate(MODES):
            rr=[lookup[c,s,m] for s in SCENARIOS]
            for ax,field,label in zip(row,fields,labels):
                ax.plot(SCENARIOS,[r[field] for r in rr],marker='o',label=LABELS[i],color=colors[i])
                ax.set(title=f'{c} executor cores',ylabel=label);ax.tick_params(axis='x',rotation=30)
        row[0].legend(fontsize=8)
    fig.tight_layout();fig.savefig(out/'resources.png',dpi=160);plt.close(fig)
    fig,axes=plt.subplots(1,len(cores),figsize=(5*len(cores),4),squeeze=False)
    for ax,c in zip(axes.flat,cores):
        for i,m in enumerate(MODES[:3]):
            rr=[lookup[c,s,m] for s in SCENARIOS]
            ax.plot(SCENARIOS,[r['event_task_max_mean_ratio_p50'] for r in rr],marker='o',label=LABELS[i],c=colors[i])
        ax.set(title=f'{c} executor cores',ylabel='Task max / mean duration');ax.tick_params(axis='x',rotation=30);ax.legend()
    fig.tight_layout();fig.savefig(out/'task_imbalance.png',dpi=160);plt.close(fig)


if __name__=='__main__':
    p=argparse.ArgumentParser()
    p.add_argument('directories',nargs='+',type=Path)
    p.add_argument('--output',required=True,type=Path)
    p.add_argument('--plots',action='store_true')
    a=p.parse_args()
    summary,records=analyze(a.directories,a.output)
    if a.plots:
        plots(summary,records,a.output)
    print(f'Analyzed {len(records)} measured runs into {a.output}')
