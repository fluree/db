.PHONY: all deps jar install deploy nodejs browser webworker cljtest cljstest test clean

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

out:
	mkdir out

package-lock.json node_modules: package.json
	npm install && touch package-lock.json node_modules

out/flureenjs.js: out package.json package-lock.json node_modules build-nodejs.edn deps.edn src/deps.cljs $(SOURCES) $(NODEJS_SOURCES) $(RESOURCES)
	clojure -M:nodejs && cp out/nodejs/flureenjs.js out/flureenjs.js

nodejs: out/flureenjs.js

out/flureedb.js: out package.json package-lock.json node_modules build-browser.edn deps.edn src/deps.cljs $(SOURCES) $(BROWSER_SOURCES) $(RESOURCES)
	clojure -M:browser && cp out/browser/main.js out/flureedb.js

browser: out/flureedb.js

out/flureeworker.js: out package.json package-lock.json node_modules build-webworker.edn deps.edn src/deps.cljs $(SOURCES) $(WEBWORKER_SOURCES) $(RESOURCES)
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
	clojure -M:docs $(@D)

docs/%.html: docs/%.md
	clojure -M:docs $(@D)

docs: docs/fluree.db.api.html docs/index.html $(DOCS_TARGETS)

cljstest: node_modules package-lock.json
	clojure -M:cljstest

cljtest:
	clojure -M:cljtest

test: cljtest cljstest

clean:
	rm -rf target
	rm -rf out
	rm -rf docs/*.html
	rm -rf node_modules
