.PHONY: help all deps jar install deploy nodejs browser webworker cljtest	\
	cljs-browser-test cljs-node-test cljstest test ci clean			\
	js-packages sync-package-json publish-nodejs publish-browser		\
	publish-webworker publish-js pending-tests pt clj-kondo-lint            \
	clj-kondo-lint-ci cljfmt-check cljfmt-fix

.DEFAULT_GOAL := help

help: ## Describe available tasks
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}'

DOCS_MARKDOWN := $(shell find docs -name '*.md')
DOCS_TARGETS := $(DOCS_MARKDOWN:docs/%.md=docs/%.html)

SOURCES := $(shell find src)
RESOURCES := $(shell find resources)
BROWSER_SOURCES := src/fluree/sdk/browser.cljs
NODEJS_SOURCES := src/fluree/sdk/node.cljs
WEBWORKER_SOURCES := src/fluree/sdk/webworker.cljs
ALL_SOURCES := $(SOURCES) $(BROWSER_SOURCES) $(WEBWORKER_SOURCES) $(NODEJS_SOURCES)

all: jar browser nodejs webworker js-packages docs ## Build all artifacts (JAR, JS packages, docs)

target/fluree-db.jar: out node_modules src/deps.cljs $(ALL_SOURCES) $(RESOURCES)
	clojure -T:build jar

jar: target/fluree-db.jar ## Build Clojure JAR

package-lock.json node_modules: package.json
	npm install && touch package-lock.json node_modules

out/fluree-node-sdk.js: package.json package-lock.json node_modules deps.edn src/deps.cljs shadow-cljs.edn $(SOURCES) $(NODEJS_SOURCES) $(RESOURCES)
	npx shadow-cljs release fluree-node-sdk && cp out/nodejs/fluree-node-sdk.js out/fluree-node-sdk.js
	@if [ ! -f out/nodejs/package.json ]; then \
		echo '{"name": "@fluree/fluree-node-sdk", "version": "3.0.0-alpha2", "main": "fluree-node-sdk.js"}' > out/nodejs/package.json; \
		echo "Created missing out/nodejs/package.json"; \
	fi

nodejs: out/fluree-node-sdk.js ## Build Node.js SDK

out/fluree-browser-sdk.js: package.json package-lock.json node_modules deps.edn src/deps.cljs shadow-cljs.edn $(SOURCES) $(BROWSER_SOURCES) $(RESOURCES)
	npx shadow-cljs release fluree-browser-sdk && cp out/browser/fluree-browser-sdk.js out/fluree-browser-sdk.js

browser: out/fluree-browser-sdk.js ## Build browser SDK

out/fluree-webworker.js: package.json package-lock.json node_modules deps.edn src/deps.cljs shadow-cljs.edn $(SOURCES) $(WEBWORKER_SOURCES) $(RESOURCES)
	npx shadow-cljs release fluree-webworker && cp out/webworker/fluree-webworker.js out/fluree-webworker.js

webworker: out/fluree-webworker.js ## Build webworker SDK

deps: ## Download and cache dependencies
	clojure -A:cljtest:cljstest:docs -P

src/deps.cljs: package.json
	clojure -M:js-deps

install: target/fluree-db.jar ## Install JAR to local repository
	clojure -T:build install

deploy: target/fluree-db.jar ## Deploy JAR to remote repository
	clojure -T:build deploy

js-packages/nodejs/package.json: package.json build.clj
	clojure -T:build sync-package-json :target $(@D)/package.json :node? true

js-packages/browser/package.json: package.json build.clj
	clojure -T:build sync-package-json :target $(@D)/package.json

js-packages/webworker/package.json: package.json build.clj
	clojure -T:build sync-package-json :target $(@D)/package.json

js-packages/nodejs/fluree-node-sdk.js: out/fluree-node-sdk.js
	cp $< $@

js-packages/browser/fluree-browser-sdk.js: out/fluree-browser-sdk.js
	cp $< $@

js-packages/webworker/fluree-webworker.js: out/fluree-webworker.js
	cp $< $@

js-packages: sync-package-json js-packages/nodejs/fluree-node-sdk.js js-packages/browser/fluree-browser-sdk.js js-packages/webworker/fluree-webworker.js ## Build all JavaScript packages

sync-package-json: js-packages/nodejs/package.json js-packages/browser/package.json js-packages/webworker/package.json ## Sync package.json files for JS packages

NPM_TAG ?= latest

publish-nodejs: js-packages/nodejs/fluree-node-sdk.js js-packages/nodejs/package.json ## Publish Node.js SDK to npm
	cd $(<D) && npm publish --tag $(NPM_TAG)

publish-browser: js-packages/browser/fluree-browser-sdk.js js-packages/browser/package.json ## Publish browser SDK to npm
	cd $(<D) && npm publish --tag $(NPM_TAG)

publish-webworker: js-packages/webworker/fluree-webworker.js js-packages/webworker/package.json ## Publish webworker SDK to npm
	cd $(<D) && npm publish --tag $(NPM_TAG)

publish-js: publish-nodejs publish-browser publish-webworker ## Publish all JavaScript packages to npm

docs/fluree.db.api.html docs/index.html: src/fluree/db/api.cljc
	clojure -T:build docs :output-path "\"$(@D)\""

docs/%.html: docs/%.md
	clojure -T:build docs :output-path "\"$(@D)\""

docs: docs/fluree.db.api.html docs/index.html $(DOCS_TARGETS) ## Generate API documentation

cljs-browser-test: node_modules package-lock.json ## Run ClojureScript browser tests
	npx shadow-cljs release browser-test
	./node_modules/karma/bin/karma start --single-run

cljs-node-test: node_modules package-lock.json ## Run ClojureScript Node.js tests
	npx shadow-cljs release node-test

nodejs-test: out/fluree-node-sdk.js ## Run Node.js SDK integration tests
	@echo "Checking if SDK files exist..."
	@ls -la out/nodejs/fluree-node-sdk.js || echo "WARNING: out/nodejs/fluree-node-sdk.js not found"
	@ls -la out/nodejs/package.json || echo "WARNING: out/nodejs/package.json not found"
	@ls -la out/fluree-node-sdk.js || echo "WARNING: out/fluree-node-sdk.js not found"
	@echo "Contents of out/nodejs/package.json:"
	@cat out/nodejs/package.json || echo "ERROR: Could not read out/nodejs/package.json"
	cd test/nodejs && npm install && npm test

browser-test: out/fluree-browser-sdk.js ## Run browser SDK integration tests
	cd test/browser && npm install && CI=true npm test

cljstest: cljs-browser-test cljs-node-test ## Run all ClojureScript tests

cljtest: ## Run Clojure tests
	clojure -X:dev:cljtest

pending-tests: ## Run pending tests
	clojure -X:dev:pending-tests

pt: pending-tests ## Alias for pending-tests

clj-kondo-lint: ## Lint Clojure code with clj-kondo
	clj-kondo --lint src:test:build.clj

clj-kondo-lint-ci:
	clj-kondo --lint src:test:build.clj --config .clj-kondo/ci-config.edn

cljfmt-check: ## Check Clojure formatting with cljfmt
	cljfmt check src dev test build.clj

cljfmt-fix: ## Fix Clojure formatting errors with cljfmt
	cljfmt fix src dev test build.clj

test: cljtest cljstest nodejs-test browser-test ## Run all tests

eastwood: ## Run Eastwood linter
	clojure -M:dev:cljtest:eastwood

ci: test clj-kondo-lint-ci cljfmt-check ## Run all CI checks (tests, linting, formatting)

clean: ## Remove build artifacts and caches
	clojure -T:build clean
	rm -rf out/*
	rm -rf docs/*.html
	rm -rf node_modules
	rm -rf test/nodejs/store
	rm -rf .shadow-cljs
	rm -rf js-packages/browser/fluree-browser-sdk.js
	rm -rf js-packages/nodejs/fluree-node-sdk.js
	rm -rf js-packages/webworker/fluree-webworker.js
