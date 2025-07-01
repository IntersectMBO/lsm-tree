set term png size 1800, 1200
set output "fpr.png"
set title "Bloom filter false positive rates (FPR) vs bits per entry\nclassic and block-structured implementations"
# set subtitle "blah"

set xlabel "Bits per entry"
set xrange [1:25]
set grid xtics
set xtics 0,2,24

set ylabel "False Positive Rate (FPR), log scale"
set yrange [1e-5:1]
set logscale y
set format y "10^{%L}"
set grid ytics

plot "fpr.classic.gnuplot.data" using 1 : 3 title "Classic, actual FPR" with points pointtype 1 pointsize 2, \
     "fpr.classic.gnuplot.data" using 1 : 2 title "Classic, calculated FPR" with lines linewidth 2, \
     "fpr.blocked.gnuplot.data" using 1 : 3 title "Blocked, actual FPR" with points pointtype 1 pointsize 2, \
     "fpr.blocked.gnuplot.data" using 1 : 2 title "Blocked, calculated FPR" with lines linewidth 3
