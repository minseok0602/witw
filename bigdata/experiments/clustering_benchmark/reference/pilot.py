import benchmark as b
import sys, time, random, statistics,csv
from collections import defaultdict
from concurrent.futures import ProcessPoolExecutor
import multiprocessing as mp
import pilot_kernel
# Use copied numerical kernel only. Spark is not present. No Spark overhead measured.
def task(item):
    sys.modules['issue_clustering']=pilot_kernel
    return b.kernel(*item)
def main():
    records=[];cal=[]
    profiles_cal={'small':[25]*4,'medium':[120]*4,'balanced':[300]*4,'skew':[1000,40,40,40],'single':[600]}
    profiles_test={'small':[32]*4,'balanced':[250]*4,'skew':[850,35,35,35],'single':[450],'many_small':[40]*12}
    with ProcessPoolExecutor(max_workers=4,mp_context=mp.get_context('spawn')) as pool:
        def run(groups,actual):
            t=time.perf_counter();res=list(map(task,groups)) if actual=='as_is' else list(pool.map(task,groups));return time.perf_counter()-t,res
        def groups_for(counts,seed):
            d=defaultdict(list)
            for r in b.fixture(counts,seed):d[r[1]].append(r)
            return sorted(d.items())
        warm=groups_for([25]*8,42);run(warm,'as_is');run(warm,'to_be_1')
        for phase,profiles in [('calibration',profiles_cal),('holdout',profiles_test)]:
            for i,(name,counts) in enumerate(profiles.items()):
                groups=groups_for(counts,1000+i+(8000 if phase=='holdout' else 0));times=defaultdict(list);dg0=None
                for rep in range(3):
                    modes=['as_is','to_be_1']+(['to_be_2'] if phase=='holdout' else []);random.Random(100+i+rep).shuffle(modes)
                    for mode in modes:
                        t=time.perf_counter();actual=b.route(counts,cal) if mode=='to_be_2' else mode;route_s=time.perf_counter()-t
                        elapsed,res=run(groups,actual);elapsed+=route_s;dg,n,assigned=b.fingerprint(res);dg0=dg0 or dg;assert dg==dg0
                        records.append(dict(phase=phase,scenario=name,counts=';'.join(map(str,counts)),rep=rep,mode=mode,actual=actual,wall_s=elapsed,kernel_sum_s=sum(x[2] for x in res),kernel_max_s=max(x[2] for x in res),kernel_cpu_s=sum(x[3] for x in res),route_s=route_s,digest=dg,clusters=n,assigned=assigned));times[mode].append(elapsed)
                if phase=='calibration':cal.append((counts,statistics.median(times['as_is']),statistics.median(times['to_be_1'])))
                print(phase,name,{m:round(statistics.median(v),4) for m,v in times.items()},flush=True)
                with open('pilot_results.csv','w',newline='') as f:
                    w=csv.DictWriter(f,fieldnames=list(records[0]));w.writeheader();w.writerows(records)
if __name__=='__main__':main()
