#!/usr/bin/env bash
# Run on an existing x86_64 Linux Docker host. Does not create AWS resources.
set -euo pipefail
here=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
lab=$(dirname "$here")
repository=$(cd "$lab/../../.." && pwd)
tag=${1:-$(date -u +%Y%m%d-%H%M%S)}
compose=(docker compose -f "$lab/compose.yml")
if ! docker image inspect news-spark:4.2.0-kafka >/dev/null 2>&1; then
  docker build -t news-spark:4.2.0-kafka "$repository/bigdata/spark"
fi
if ! docker image inspect witw-clustering-lab:local >/dev/null 2>&1; then
  docker build -f "$lab/Dockerfile" -t witw-clustering-lab:local "$lab"
fi
mkdir -p "$lab/results"
trap '"${compose[@]}" down' EXIT
model=()
for cores in 1 2 4; do
  export LAB_CORES=$cores LAB_RUN="partition-mini-$cores-$tag"
  test ! -e "$lab/results/$LAB_RUN" || { echo "Output already exists: $LAB_RUN"; exit 1; }
  "${compose[@]}" up -d master worker
  "${compose[@]}" run -d --name witw-clustering-lab-mini --use-aliases --no-deps driver \
    /opt/spark/bin/spark-submit --master spark://master:7077 --driver-memory 1536m --executor-memory 3g \
    --conf "spark.cores.max=$cores" --conf "spark.executor.cores=$cores" \
    --conf spark.driver.host=driver --conf spark.driver.bindAddress=0.0.0.0 \
    --conf spark.executor.processTreeMetrics.enabled=true --conf spark.executor.metrics.pollingInterval=250 \
    --conf spark.eventLog.logStageExecutorMetrics=true \
    /bench/partitioning/mini_benchmark.py --source /source --cores "$cores" --output "/results/$LAB_RUN" "${model[@]}"
  docker inspect witw-clustering-lab-mini witw-clustering-lab-worker-1 witw-clustering-lab-master-1 > "$lab/results/$LAB_RUN.containers.json"
  code=$(docker wait witw-clustering-lab-mini)
  "${compose[@]}" stop worker
  docker cp "witw-clustering-lab-mini:/results/$LAB_RUN" "$lab/results/"
  docker logs witw-clustering-lab-mini > "$lab/results/$LAB_RUN.console.log" 2>&1
  docker rm witw-clustering-lab-mini
  test "$code" = 0 || { echo "Mini test failed: $LAB_RUN"; exit 1; }
  docker run --rm --network none --entrypoint python3 \
    -v "$lab:/bench:ro" -v "$lab/results:/artifacts" witw-clustering-lab:local \
    /bench/partitioning/validate_mini.py "/artifacts/$LAB_RUN"
  if [ "${#model[@]}" = 0 ]; then model=(--cost-model "/results/$LAB_RUN/cost_model.json"); fi
done
