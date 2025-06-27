set -ex

BATCH_COUNT=$1

cd _bench_wp8_oneshot
awk 'NR==1; NR>1 {lookups += $3; updates += $4; print $1"\t"$2"\t"lookups"\t"updates}' \
  latency-$BATCH_COUNT.dat > latency-cumulative-$BATCH_COUNT.dat
gnuplot latency-$BATCH_COUNT.gp
gnuplot latency-cumulative-$BATCH_COUNT.gp
cd ..

cd _bench_wp8_incremental
awk 'NR==1; NR>1 {lookups += $3; updates += $4; print $1"\t"$2"\t"lookups"\t"updates}' \
  latency-$BATCH_COUNT.dat > latency-cumulative-$BATCH_COUNT.dat
gnuplot latency-$BATCH_COUNT.gp
gnuplot latency-cumulative-$BATCH_COUNT.gp
cd ..

cd _bench_wp8_greedy
awk 'NR==1; NR>1 {lookups += $3; updates += $4; print $1"\t"$2"\t"lookups"\t"updates}' \
  latency-$BATCH_COUNT.dat > latency-cumulative-$BATCH_COUNT.dat
gnuplot latency-$BATCH_COUNT.gp
gnuplot latency-cumulative-$BATCH_COUNT.gp
cd ..
