set title "Cumulative latency (Incremental)"

set xlabel "Batch number"

set ylabel "Time (nanoseconds)"
set yrange [0:700000000000]
set y2label "Number of runs"
set y2range [0:70]
set y2tics 10

set terminal png size 1200,800
set output "latency-cumulative-200000.png"

plot "latency-cumulative-200000.dat" using 1:3 title 'lookups' axis x1y1 with lines, \
     "latency-cumulative-200000.dat" using 1:4 title 'updates' axis x1y1 with lines, \
     "latency-cumulative-200000.dat" using 1:2 title 'runs'    axis x1y2 with lines
