SHELL := /bin/bash
.ONESHELL:
.DELETE_ON_ERROR:
.SHELLFLAGS := -eu -o pipefail -c
MAKEFLAGS += --warn-undefined-variables
MAKEFLAGS += --no-builtin-rules


ifeq ($(OS), Windows_NT)
    DETECTED_OS := Windows
    DC_CLI_COMMAND := .\.tools\dc.exe
else
    DETECTED_OS := $(shell sh -c 'uname 2>/dev/null || echo Unknown')
    DC_CLI_COMMAND := ./.tools/dc
endif

NPM_ACCESS_TOKEN ?=

.DEFAULT_GOAL := build

.PHONY: restore
restore:
	yarn

.PHONY: build
build: clean restore
	yarn build

.PHONY: test
test: restore
	yarn test

.PHONY: clean
clean:
	rm -rf ./node_modules ./dist ./index.js ./index.d.ts

.PHONY: publish
publish: build
ifdef NPM_ACCESS_TOKEN
	npm config set //registry.npmjs.org/:_authToken $(NPM_ACCESS_TOKEN) && npm publish . --ignore-scripts --access public
endif
