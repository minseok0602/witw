# explore measurements

|App|Distribution|Strategy|n|p50 s|p95 s|CV Task p50|Speedup|
|---|---|---|---:|---:|---:|---:|---:|
|A|SKEW|A0|3|21.1290|21.3947|0.714|1.000|
|A|SKEW|A1|3|11.1996|11.2044|0.005|1.887|
|A|SKEW|A2|3|11.0357|11.0923|0.005|1.915|
|B|SKEW|B0|3|1.1549|1.1777|0.955|1.000|
|B|SKEW|B1|3|0.6647|0.7923|0.106|1.737|
|B|SKEW|B2|3|0.6042|0.9205|0.089|1.912|

Exploratory/quick values do not establish a winning strategy. Task sums and JVM metric sums are not elapsed overhead. Worker idle capacity is inferred from 250ms cgroup samples. Local container network counters are not measurements of inter-server EC2 traffic.
