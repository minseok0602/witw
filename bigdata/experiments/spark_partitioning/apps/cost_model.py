"""Choose a cost model by independent calibration-validation error, never A/B holdout data."""
import json
import statistics
from pathlib import Path
import numpy as np
from clustering import kernel
from data import clustering_rows

SIZES=[25,50,100,200,400,800,1200,1600,2000]


def interpolate(n,sizes,values):
    return float(np.interp(min(2000,n),sizes,values))


def predict(n,model):
    x=min(2000,n)
    if model['selected']=='linear':return max(1e-6,model['linear'][0]*x+model['linear'][1])
    if model['selected']=='square':return max(1e-6,model['square'][0]*x*x+model['square'][1])
    return max(1e-6,interpolate(x,model['sizes'],model['empirical']))


def calibration_task(item):
    import socket
    from pyspark import TaskContext
    phase,n,seed,repeat,partition=item
    result=kernel(1,clustering_rows({1:n},seed))
    context=TaskContext.get()
    return dict(phase=phase,n=n,seed=seed,rep=repeat,partition=partition,host=socket.gethostname(),
                stage_id=context.stageId(),task_id=context.taskAttemptId(),wall_s=result['wall_s'],cpu_s=result['cpu_s'])


def calibrate(out,repeats=2,sc=None):
    from scipy.optimize import nnls
    if sc is None:raise ValueError('Calibration requires the actual two-executor Spark context')
    records=[]
    # Initialize sklearn/native kernels in each reusable Python worker before fitting.
    # These Tasks are visible in the Event Log but excluded from calibration samples.
    for rep in range(2):
        sc.setJobGroup(f'calibrate-warmup-{rep}','excluded calibration worker warmup')
        warmed=sc.parallelize([('warmup',32,600000+rep*10+pid,rep,pid) for pid in range(4)],4).map(calibration_task).collect()
        assert len({r['host'] for r in warmed})==2
    # Four simultaneous single-CPU Tasks reproduce the fixed worker execution environment.
    for phase,offset in [('fit',610000),('model_validation',620000)]:
        for i,n in enumerate(SIZES):
            for rep in range(repeats):
                sc.setJobGroup(f'calibrate-{phase}-{n}-{rep}','independent executor cost-model calibration')
                descriptors=[(phase,n,offset+i*100+rep*10+pid,rep,pid) for pid in range(4)]
                result=sc.parallelize(descriptors,4).map(calibration_task).collect()
                assert len(result)==4 and len({r['host'] for r in result})==2
                records.extend(result)
    sc.setLocalProperty('spark.jobGroup.id',None)
    means=lambda phase:[statistics.median(r['wall_s'] for r in records if r['phase']==phase and r['n']==n) for n in SIZES]
    train=np.array(means('fit'));test=np.array(means('model_validation'));x=np.array(SIZES,dtype=float)
    linear=nnls(np.column_stack([x,np.ones(len(x))]),train)[0]
    square=nnls(np.column_stack([x*x,np.ones(len(x))]),train)[0]
    candidates={'linear':linear[0]*x+linear[1],'square':square[0]*x*x+square[1],'empirical':train}
    errors={name:dict(mae_s=float(np.mean(np.abs(y-test))),mape=float(np.mean(np.abs(y-test)/np.maximum(test,1e-9))))
            for name,y in candidates.items()}
    selected=min(errors,key=lambda name:errors[name]['mae_s'])
    model=dict(selected=selected,linear=linear.tolist(),square=square.tolist(),sizes=SIZES,empirical=train.tolist(),
        errors=errors,selection_metric='independent model-validation executor wall-time MAE seconds',
        calibration_environment='two actual executors x two cores, four simultaneous single-CPU Tasks',
        excluded_warmup_tasks=8,
        calibration_seeds=[r['seed'] for r in records],benchmark_holdout_used=False,dimension=768,
        note='Executor wall-time approximation for this implementation and synthetic density; no complexity guarantee')
    (Path(out)/'cost_model.json').write_text(json.dumps(model,indent=2))
    from event_metrics import write_csv
    write_csv(Path(out)/'cost_calibration.csv',records)
    return model
