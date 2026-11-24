# 의도적 Task 부하 편향 미니 테스트

기존 탐색의 테마 기사 수 편향과 구분하는 추가 실험이다. 운영 코드·기존 결과는 그대로 보존한다. 각 전략 3회, 총18회만 로컬에서 측정한다. 기존과 같은2 Executor×2 Core,4파티션,768차원,파라미터·라이브러리·스레드·워밍업·계측 계약을 사용한다. 유료 EC2는 사용하지 않는다.

## A — 기사 수는 같고 후보 비교량이 다른 입력

테마별 기사500개, 전체4000개, 후보 수 `[200,100,50,10,150,80,30,20]`를 유지한다. 초기 목적지는 일반 Hash 대신 실험용 identity partitioning으로 지정한다. 비교군 모두 같은 준비된 DataFrame에서 시작한다.

|초기 파티션 그룹|테마|기사 수|후보 비교 수(차원 곱하기 전)|
|---|---|---:|---:|
|0|T1+T5|1000|175000|
|1|T2+T6|1000|90000|
|2|T3+T7|1000|40000|
|3|T4+T8|1000|15000|

최대/최소 예상 연산량 비율11.67배다. 동일한 기사 수에서 후보 수 차이로 부하를 치우치게 만든다. 반복마다 목적지번호를 회전한다. A0는 유지, A1은 기사 수 균등 round-robin 재배치, A2는 후보 비교 비용 균등 quota 재배치를 수행한다. A1도 round-robin으로 테마 구성이 섞이므로 연산량 균형이 개선될 수 있다. A2만 유리하도록 A1의 배치를 악화시키지 않는다.

## B — 큰 테마 네 개가 동일 Hash 파티션에 충돌

테마별 기사 수 `[450,400,350,300,150,150,100,100]`, 전체2000개다. 첫 반복의 ID는 `[1,5,9,13,2,3,4,8]`로 지정해 큰 네 테마가 기본 RDD 정수 Hash의 같은 파티션으로 들어가게 한다. 실제 B0 결과에서 이를 검사한다. 반복마다 ID를 같은 offset만큼 이동해 충돌 목적지를 회전한다. 전체 기사 수·테마별 크기·후보 생성 연산은 전략 간 동일하다.

B0의 파티션별 기사 수는 순서를 제외하면 `[1500,200,150,150]`이며 한 Task가450/400/350/300개 테마를 차례로 처리한다. B1은 기사 수 LPT, B2는 별도 데이터로 보정한 고정 모델의 예상 시간 LPT다. 동일 테마는 분할하지 않는다. `B-quick-executor-cal-warm/cost_model.json`을 재사용하고 이번 결과로 재조정하지 않는다.

## 실행과 해석

```powershell
& bigdata/experiments/spark_partitioning/run_local.ps1 -Application A -Mode explore -Repetitions 3 -Scenarios SKEW -RunTag deliberate-skew-v1
& bigdata/experiments/spark_partitioning/run_local.ps1 -Application B -Mode explore -Repetitions 3 -Scenarios SKEW -CostModelRun B-quick-executor-cal-warm -RunTag deliberate-skew-v1
```

초기 배치 검증은 timer 밖의 입력 준비 작업이다. A1/A2/B1/B2의 계획·추가 집계·Shuffle은 timer에 포함한다. 실제 입력 파티션 구성은 실행별 `skew-input-<seed>.json`, 연산 파티션 구성은 `partition_runs.csv`와 `plans.json`에 저장한다. `VALIDATED.json`은 결과 동일성 및 실제 편향 구성을 검사한다.

인위적인 편향에서 개선 가능성을 검증하는 테스트이며, 이러한 입력이 실제 운영에서 얼마나 자주 발생하는지를 보여주지 않는다. 3회 표본으로 운영 개선율을 확정하지 않는다.
