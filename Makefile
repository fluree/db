.PHONY: all deps jar install deploy deploy-browser deploy-jar sync-versions nodejs browser webworker cljtest cljs-browser-test cljs-node-test cljstest test eastwood ci clean

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

target/fluree-db.jar: out node_modules src/deps.cljs $(ALL_SOURCES) $(RESOURCES)
	clojure -X:jar

jar: target/fluree-db.jar

package-lock.json node_modules: package.json
	npm install && touch package-lock.json node_modules

out/flureenjs.js: package.json package-lock.json node_modules build-nodejs.edn deps.edn src/deps.cljs $(SOURCES) $(NODEJS_SOURCES) $(RESOURCES)
	clojure -M:nodejs && cp out/nodejs/flureenjs.js out/flureenjs.js

nodejs: out/flureenjs.js

out/flureedb.js: package.json package-lock.json node_modules build-browser.edn deps.edn src/deps.cljs $(SOURCES) $(BROWSER_SOURCES) $(RESOURCES)
	clojure -M:browser && cp out/browser/main.js out/flureedb.js

browser: out/flureedb.js

out/flureeworker.js: package.json package-lock.json node_modules build-webworker.edn deps.edn src/deps.cljs $(SOURCES) $(WEBWORKER_SOURCES) $(RESOURCES)
	clojure -M:webworker && cp out/webworker/main.js out/flureeworker.js

webworker: out/flureeworker.js

deps:
	clojure -A:cljtest:cljstest:eastwood:docs -P

src/deps.cljs: package.json
	clojure -M:js-deps

install: target/fluree-db.jar
	clojure -M:install

sync-versions:
	cd packages/flureedb && npm version $(VERSION) --allow-same-version
	cd packages/flureenjs && npm version $(VERSION) --allow-same-version
	cd packages/flureeworker && npm version $(VERSION) --allow-same-version

deploy-jar: target/fluree-db.jar
	clojure -M:deploy

packages/%/LICENSE: LICENSE
	cp $< $@

deploy-browser: out/flureedb.js sync-versions packages/flureedb/LICENSE
	cp out/flureedb.js packages/flureedb/
	cd packages/flureedb && npm publish

deploy-nodejs: out/flureenjs.js sync-versions packages/flureenjs/LICENSE
	tail -n +2 out/flureenjs.js > packages/flureenjs/flureenjs.bare.js # remove shebang from compiler output
	cd packages/flureenjs && sh wrap-umd.sh && npm run test && npm publish

deploy-worker: out/flureeworker.js sync-versions packages/flureeworker/LICENSE
	cp out/flureeworker.js packages/flureeworker/
	cd packages/flureeworker && npm publish

deploy: deploy-jar deploy-browser deploy-nodejs deploy-worker

docs/fluree.db.api.html docs/index.html: src/fluree/db/api.clj
	clojure -X:docs :output-path "\"$(@D)\""

docs/%.html: docs/%.md
	clojure -X:docs :output-path "\"$(@D)\""

docs: docs/fluree.db.api.html docs/index.html $(DOCS_TARGETS)

cljs-browser-test: node_modules package-lock.json
	rm -rf out/* # prevent circular dependency cljs.core -> cljs.core error
	clojure -M:cljs-browser-test

cljs-node-test: node_modules package-lock.json
	rm -rf out/* # prevent circular dependency cljs.core -> cljs.core error
	clojure -M:cljs-node-test

cljstest: cljs-browser-test cljs-node-test

cljtest:
	clojure -M:cljtest

test: cljtest cljstest

eastwood:
	clojure -M:test:eastwood

ci: test eastwood

clean:
	rm -rf target
	rm -rf out/*
	rm -rf docs/*.html
	rm -rf node_modules
