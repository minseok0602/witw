# 의도적 연산 부하 편향 — 로컬 미니 실측

2026-11-24, 각 전략3회, 총18회. 워밍업 제외. 두 Executor×2 Core,4파티션으로 동일하게 실행했다. 운영 파일과 데이터는 변경하지 않았고 EC2는 사용하지 않았다. 이전 D1~D4 결과와 섞지 않은 별도 실행이다.

## 구성

- A:4,000건, 초기 파티션마다1,000건. 후보 비교 수를175,000/90,000/40,000/15,000회로 구성해 최대/최소11.67배 차이를 만들었다. A0는 이 배치를 유지하고 A1/A2가 재배치했다.
- B:2,000건,8개 테마.450/400/350/300건 테마4개가 같은 Hash 파티션에 충돌하도록 ID를 정했다. B0의 실제 파티션 기사 수는 순서를 제외하면1,500/200/150/150건이다. B1/B2는 테마를 분할하지 않고 재배치했다.
- 매 반복에서 초기 편향의 물리 목적지를 회전하고 전략 순서를 무작위로 바꿨다. 동일 반복은 완전히 동일한 기사·임베딩·시드를 사용했다. B2 모델은 이전 독립 보정 모델을 동결해 재사용했다.

## 결과

초 단위, n=3이다. 전체 시간에는 추가 집계·계획·Shuffle·원본 연산·동일 방식의 최종 collect가 포함된다.

|실험|전략|p50 초|p95 초|Task 시간 CV 중앙값|기준 대비 Speedup|
|---|---|---:|---:|---:|---:|
|A|A0 입력 유지|21.1290|21.3947|0.714|1.000|
|A|A1 기사 수 균등|11.1996|11.2044|0.005|1.887|
|A|A2 후보 비용 균등|11.0357|11.0923|0.005|1.915|
|B|B0 기본 Hash|1.1549|1.1777|0.955|1.000|
|B|B1 기사 수 LPT|0.6647|0.7923|0.106|1.737|
|B|B2 예상 시간 LPT|0.6042|0.9205|0.089|1.912|

A/B 모두 실제 Task 편향이 줄었고 전체 시간이 감소했다. A2 대 A0의 p50 감소는 약47.8%, B2 대 B0는 약47.7%다. 이는 의도적으로 만든 편향에서 개선 가능성을 확인한 결과다. 실제 운영 입력에서 이러한 조건이 얼마나 자주 발생하는지는 측정하지 않았다.

A1은 기사 수 기준이지만 round-robin으로 기사를 다시 섞으므로 이번 구성에서는 후보 비용도 함께 균등해졌다. 따라서 A2의 추가 비용 추정이 반드시 필요하다는 결론은 낼 수 없다. B2는 p50이 낮지만 p95는 B1보다 높았으며3회 표본으로 B1/B2의 우열이나 안정성을 확정하지 않는다.

## 검증과 원시 자료

두 실행의 `VALIDATED.json` 모두 passed=true, 결과 동일, 실제 Executor [2,2], 연산 Stage4 Task, 실패 Task0이다. A는 기사별 할당·미분류·최고 유사도 등을 원본 참조와 비교했고 B는 입력·군집 멤버·대표·centroid·confidence를 비교했다. 동일 테마 분할이 없고 계획한 목적지와 실제 Task가 일치했다. 부동소수점 정규화는 기존 실험 계약을 유지했다.

- [정확한 설정 및 재현 명령](DELIBERATE_SKEW_SETUP.md)
- [분석 표](../results/deliberate-skew-analysis/MEASUREMENTS.md), [요약 CSV](../results/deliberate-skew-analysis/summary.csv), [paired CSV](../results/deliberate-skew-analysis/paired.csv)
- [A 원시 측정](../results/A-explore-deliberate-skew-v1/runs.csv), [A 실제 배치](../results/A-explore-deliberate-skew-v1/partition_runs.csv), [A 검증](../results/A-explore-deliberate-skew-v1/VALIDATED.json)
- [B 원시 측정](../results/B-explore-deliberate-skew-v1/runs.csv), [B 실제 배치](../results/B-explore-deliberate-skew-v1/partition_runs.csv), [B 검증](../results/B-explore-deliberate-skew-v1/VALIDATED.json)

각 실행 디렉터리에 Event Log, 환경·코드 사본, 자원 샘플, `skew-input-<seed>.json`, 배치 계획을 보존했다. 이 미니 테스트로30회 본실험이나 운영 적용 검증을 대체하지 않는다.
