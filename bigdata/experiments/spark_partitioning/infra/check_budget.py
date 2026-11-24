"""Re-estimate full A/B duration from this EC2's completed Quick Tests before paid full runs."""
import csv
import argparse
import json
import time
from pathlib import Path


def main():
    parser=argparse.ArgumentParser();parser.add_argument('--remaining-reps',type=int)
    remaining_reps=parser.parse_args().remaining_reps
    if remaining_reps is None:
        completed=[Path('/opt/witw/results')/(app+'-full-session1/VALIDATED.json') for app in ['A','B']]
        remaining_reps=15 if all(p.exists() and json.loads(p.read_text())['passed'] for p in completed) else 30
    if remaining_reps not in [15,30]:raise ValueError('Only the planned half/full remaining workload is allowed')
    root=Path('/opt/witw'); details=[]; main_runs=0; extra_baselines=0
    cluster=json.loads((root/'state/cluster.json').read_text())
    for app in ['A','B']:
        candidates=sorted((root/'results').glob(app+'-quick-*/VALIDATED.json'))
        passed=[p for p in candidates if json.loads(p.read_text())['passed']]
        if not passed:raise RuntimeError('Missing validated EC2 Quick Test: '+app)
        run=passed[-1].parent
        environment=json.loads((run/'environment.json').read_text())
        main_runs+=3*len(environment['scenarios'])*remaining_reps
        if app=='B' and 'SKEW' not in environment['scenarios']:extra_baselines=2*len(environment['scenarios'])*30
        with (run/'resources-driver.csv').open() as f:samples=list(csv.DictReader(f))
        epochs=[float(r['epoch_s']) for r in samples]
        if max(b-a for a,b in zip(epochs,epochs[1:]))>5:raise RuntimeError('Interrupted Quick Test cannot estimate budget')
        with (run/'jobs.csv').open() as f:jobs=list(csv.DictReader(f))
        first=min(float(j['submission_ms'])/1000 for j in jobs if j['group'].startswith('prepare-quick-'))
        fixed=max(0,first-epochs[0]); variable=max(0,epochs[-1]-first)
        details.append(dict(app=app,run=run.name,quick_gross_s=epochs[-1]-epochs[0],fixed_s=fixed,
                            remaining_workload_projected_s=fixed+remaining_reps*variable))
    hours=float(cluster['budget_hours'])
    deadline=root/'state/deadline_epoch'
    remaining=(float(deadline.read_text())-time.time() if deadline.exists() else
               hours*3600-time.clock_gettime(time.CLOCK_BOOTTIME))
    # Factor two is a planning scenario, not a confidence interval. Include one hour for recovery/analysis.
    needed=2*sum(r['remaining_workload_projected_s'] for r in details)+3600
    result=dict(passed=needed<remaining,quick_observations=details,remaining_watchdog_s=remaining,
                full_ab_plus_reserve_s=needed,main_runs=main_runs,extra_baselines=extra_baselines,
                note='EC2 Quick extrapolation; no C run included, no runtime guarantee')
    (root/'results/EC2_BUDGET_RECHECK.json').write_text(json.dumps(result,indent=2))
    print(json.dumps(result,indent=2))
    if not result['passed']:raise RuntimeError('Projected full run exceeds remaining deadline; preserve results and request revised scope')


if __name__=='__main__':main()
