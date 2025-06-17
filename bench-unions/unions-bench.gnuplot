set term png size 1800, 1200
set output "unions-bench.png"
set title "Lookup performance on incremental union tables"
# set subtitle "blah"

set xlabel "Iteration"
set grid xtics
set xtics 0,50,150

set ylabel "Lookups per second"

plot "unions-bench.dat" using 1 : 2 title "Baseline table" axis x1y1, \
     "unions-bench.dat" using 1 : 3 title "Union table" axis x1y1, \
     "unions-bench.dat" using 1 : 4 title "Current union debt" axis x1y2
