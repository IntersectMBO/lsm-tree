set -ex

SETUP="cabal run --project-file=cabal.project.release --flag=+measure-batch-latency lsm-tree-bench-wp8 -- setup --disk-cache-policy DiskCacheNone"

mkdir _bench_wp8_oneshot
mkdir _bench_wp8_incremental
mkdir _bench_wp8_greedy

$SETUP --bench-dir _bench_wp8_oneshot     --merge-schedule OneShot
$SETUP --bench-dir _bench_wp8_incremental --merge-schedule Incremental
$SETUP --bench-dir _bench_wp8_greedy      --merge-schedule Greedy
