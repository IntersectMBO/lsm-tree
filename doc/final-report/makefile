.POSIX:

.SILENT:

.PHONY: all
all: final-report.pdf integration-notes.pdf

final-report.pdf: final-report.md pipelining.pdf
	pandoc -C -o final-report.pdf final-report.md

integration-notes.pdf: integration-notes.md
	pandoc -o integration-notes.pdf integration-notes.md

.PHONY: clean
clean:
	rm -f final-report.pdf integration-notes.pdf
