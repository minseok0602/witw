# Spark 파티셔닝 실험

- [현재 A/B 실험](spark_partitioning/README.md): 매칭은 기사 단위,군집화는 테마 단위로 부하를 배분한다.
- [로컬 편향 미니 실측](spark_partitioning/docs/DELIBERATE_SKEW_RESULT.md): 18회 정합성 통과,인위적 부하 편향의 개선 가능성을 검증했다.
- [승인된 EC2 실행 계약](spark_partitioning/docs/APPROVED_RELIABILITY_RUN.md): 기본/중간/심한 편향,두 세션,총540회,예산10,000원이다.
- [이전 실험 보존](clustering_benchmark/README.md): Driver/Executor 전환 및 Core/Task 탐색 기록이며 현재 본실험과 혼합하지 않는다.

## 이 커밋의 상태

2026-11-24 로컬 Quick 및 탐색 결과까지 포함한다. EC2540회 실험은 시작했지만 이번 커밋에는 완료 결과를 포함하지 않았다. 실행 중인 제어기·운영 코드·운영 데이터는 변경하지 않는다.

작은 원시/파생CSV,검증JSON,환경,그래프를선별 포함했다. 대용량원시EventLog·기사별결과·개인SSH키·AWS실행상태는 Git에 넣지 않고 원래 로컬디렉터리에 보존한다. [ARTIFACTS.json](ARTIFACTS.json)에 포함하지 않은 원시EventLog의 크기와SHA256을 기록했다. Git스냅샷만으로 기존EventLog를다시파싱할수는없으며,새실험을재현해새EventLog를생성할수있다.
