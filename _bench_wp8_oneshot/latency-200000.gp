set title "Latency (OneShot)"

set xlabel "Batch number"

set logscale y
set ylabel "Time (nanoseconds)"

set terminal png size 1200,800
set output "latency-200000.png"

plot "latency-200000.dat" using 1:3 title 'lookups' axis x1y1, \
     "latency-200000.dat" using 1:4 title 'updates' axis x1y1
