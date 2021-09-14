.PHONY: all deps jar install deploy nodejs browser webworker cljtest cljstest test eastwood ci clean

DOCS_MARKDOWN := $(shell find docs -name '*.md')
DOCS_TARGETS := $(DOCS_MARKDOWN:docs/%.md=docs/%.html)

SOURCES := $(shell find src)
RESOURCES := $(shell find resources)
BROWSER_SOURCES := src-cljs/flureedb.cljs
WEBWORKER_SOURCES := src-cljs/flureeworker.cljs
NODEJS_SOURCES := $(shell find src-nodejs)
ALL_SOURCES := $(SOURCES) $(BROWSER_SOURCES) $(WEBWORKER_SOURCES) $(NODEJS_SOURCES)

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
	clojure -A:cljtest:cljstest -P

src/deps.cljs: package.json
	clojure -M:js-deps

install: target/fluree-db.jar
	clojure -M:install

deploy: target/fluree-db.jar
	clojure -M:deploy

docs/fluree.db.api.html docs/index.html: src/fluree/db/api.clj
	clojure -X:docs "{:output-path $(@D)}"

docs/%.html: docs/%.md
	clojure -X:docs "{:output-path $(@D)}"

docs: docs/fluree.db.api.html docs/index.html $(DOCS_TARGETS)

cljstest: node_modules package-lock.json
	clojure -M:cljstest

cljtest:
	clojure -M:cljtest

test: cljtest cljstest

eastwood:
	clojure -M:test:docs:eastwood

ci: test eastwood

clean:
	rm -rf target
	rm -rf out/*
	rm -rf docs/*.html
	rm -rf node_modules
