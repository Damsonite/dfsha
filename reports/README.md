# DFSha benchmark

`reports/benchmark.json` was generated with:

```bash
python -m pytest -q
python -m scripts.benchmark \
  --sizes 1,4,8,16 \
  --runs 3 \
  --metadata-runs 50 \
  --output reports/benchmark.json
```

Environment:

- Python 3.14.10
- Linux x86_64
- Docker Compose running on a single local host
- 4 MiB blocks
- replication factor 3
- write quorum 2
- 150 virtual nodes

## Results

| Operation | Runs | Avg ms | p50 ms | p95 ms | Max ms |
|---|---:|---:|---:|---:|---:|
| `Stat` | 50 | 3.237 | 3.112 | 4.870 | 5.468 |
| `ResolvePath` | 50 | 4.219 | 4.088 | 5.488 | 8.058 |
| `ListDir` | 50 | 4.547 | 4.165 | 6.333 | 10.078 |

| File size | Blocks | Upload avg s | Upload MiB/s | Download avg s | Download MiB/s |
|---:|---:|---:|---:|---:|---:|
| 1 MiB | 1 | 0.047 | 21.075 | 0.024 | 42.551 |
| 4 MiB | 1 | 0.095 | 42.064 | 0.046 | 86.334 |
| 8 MiB | 2 | 0.186 | 42.941 | 0.071 | 112.149 |
| 16 MiB | 4 | 0.321 | 49.883 | 0.132 | 121.614 |

These numbers measure the whole stack: authentication, lease acquisition,
block replication to the three Storage Nodes, metadata commit, SHA-256
validation and the client streaming protocol. They are local Docker results,
not AWS Academy results. Network latency, instance type and EBS settings will
change them significantly.

The benchmark creates a unique directory, uploads and verifies each file,
removes each file, and removes the directory at the end.
