.PHONY: all deps jar install deploy deploy-browser deploy-jar sync-version nodejs \
        browser webworker cljtest cljs-browser-test cljs-node-test cljstest test \
        eastwood ci clean-browser clean-node clean-webworker clean

DOCS_MARKDOWN := $(shell find docs -name '*.md')
DOCS_TARGETS := $(DOCS_MARKDOWN:docs/%.md=docs/%.html)

SOURCES := $(shell find src)
RESOURCES := $(shell find resources)
BROWSER_SOURCES := src-cljs/flureedb.cljs
WEBWORKER_SOURCES := src-cljs/flureeworker.cljs
NODEJS_SOURCES := $(shell find src-nodejs)
ALL_SOURCES := $(SOURCES) $(BROWSER_SOURCES) $(WEBWORKER_SOURCES) $(NODEJS_SOURCES)

VERSION := $(shell clojure -M:meta version)

all: jar browser nodejs webworker docs

target/fluree-db.jar: js node_modules src/deps.cljs $(ALL_SOURCES) $(RESOURCES)
	clojure -X:jar

jar: target/fluree-db.jar

package-lock.json node_modules: package.json
	npm install && touch package-lock.json node_modules

js/nodejs/index.js: package.json package-lock.json node_modules build-nodejs.edn deps.edn src/deps.cljs $(SOURCES) $(NODEJS_SOURCES) $(RESOURCES)
	clojure -M:nodejs && cd $(@D) && ./wrap-umd.sh
	npm run test

nodejs: js/nodejs/index.js

js/browser/index.js: package.json package-lock.json node_modules build-browser.edn deps.edn src/deps.cljs $(SOURCES) $(BROWSER_SOURCES) $(RESOURCES)
	clojure -M:browser

browser: js/browser/index.js

js/webworker/index.js: package.json package-lock.json node_modules build-webworker.edn deps.edn src/deps.cljs $(SOURCES) $(WEBWORKER_SOURCES) $(RESOURCES)
	clojure -M:webworker

webworker: js/webworker/index.js

deps:
	clojure -A:cljtest:cljstest:eastwood:docs -P

src/deps.cljs: package.json
	clojure -M:js-deps

install: target/fluree-db.jar
	clojure -M:install

sync-version:
	npm version $(VERSION) --allow-same-version --no-git-tag-version --force

deploy: target/fluree-db.jar
	clojure -M:deploy

docs/fluree.db.api.html docs/index.html: src/fluree/db/api.clj
	clojure -X:docs :output-path "\"$(@D)\""

docs/%.html: docs/%.md
	clojure -X:docs :output-path "\"$(@D)\""

docs: docs/fluree.db.api.html docs/index.html $(DOCS_TARGETS)

cljs-browser-test: node_modules package-lock.json clean-cljs-test
	clojure -M:cljs-browser-test

cljs-node-test: node_modules package-lock.json
	clojure -M:cljs-node-test

cljstest: cljs-browser-test cljs-node-test

cljtest:
	clojure -M:cljtest

test: cljtest cljstest

eastwood:
	clojure -M:test:eastwood

ci: test eastwood

clean-browser:
	rm -rf js/browser/index.js
	rm -rf js/browser/webpack.js
	rm -rf js/browser/build

clean-node:
	rm -rf js/nodejs/index.js
	rm -rf js/nodejs/webpack.js
	rm -rf js/nodejs/build

clean-webworker:
	rm -rf js/webworker/index.js
	rm -rf js/webworker/webpack.js
	rm -rf js/webworker/build

clean-cljs-test:
	rm -rf js/test/index.js
	rm -rf js/test/webpack.js
	rm -rf js/test/build

clean: clean-browser clean-node clean-webworker clean-cljs-test
	rm -rf target
	rm -rf node_modules
