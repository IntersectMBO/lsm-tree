set -ex

BATCH_COUNT=$1

RUN="cabal run --project-file=cabal.project.release --flag=+measure-batch-latency lsm-tree-bench-wp8 -- run --disk-cache-policy DiskCacheNone --batch-count $BATCH_COUNT"

rm -f latency.dat latency.gp

$RUN --bench-dir _bench_wp8_oneshot     | tee _bench_wp8_oneshot/output-$BATCH_COUNT.txt
mv latency.dat _bench_wp8_oneshot/latency-$BATCH_COUNT.dat

$RUN --bench-dir _bench_wp8_incremental | tee _bench_wp8_incremental/output-$BATCH_COUNT.txt
mv latency.dat _bench_wp8_incremental/latency-$BATCH_COUNT.dat

$RUN --bench-dir _bench_wp8_greedy      | tee _bench_wp8_greedy/output-$BATCH_COUNT.txt
mv latency.dat _bench_wp8_greedy/latency-$BATCH_COUNT.dat
