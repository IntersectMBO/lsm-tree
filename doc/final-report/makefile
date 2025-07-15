.POSIX:

.SUFFIXES:

.PHONY: all
all: final-report.pdf integration-notes.pdf

final-report.pdf: final-report.md ieee-software.csl pipelining.pdf
	pandoc --citeproc $(@:.pdf=.md) -o $@

integration-notes.pdf: integration-notes.md
	pandoc $(@:.pdf=.md) -o $@

.PHONY: clean
clean:
	rm -f final-report.pdf integration-notes.pdf
