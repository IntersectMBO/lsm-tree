SOURCE=lsm-tree.cabal $(shell find src -type f -name '*.hs')

.PHONY: watch
watch:
	fswatch -o $(SOURCE) | xargs -n1 -I{} make build

.PHONY: build
build: $(SOURCE)
	time cabal haddock lsm-tree:lib:lsm-tree --builddir="dist-newstyle/haddock"

.PHONY: serve
serve:
	python -m http.server -d "dist-newstyle/haddock/build/"*"/ghc-"*"/lsm-tree-"*"/doc/html/lsm-tree/"
