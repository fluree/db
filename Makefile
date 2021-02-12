.PHONY: deps jar install deploy nodejs browser webworker cljtest cljstest test clean docs

DOCS_MARKDOWN := $(shell find doc -name '*.md')
DOCS_TARGETS := $(DOCS_MARKDOWN:doc/%.md=doc/clj/%.html)

SOURCES := $(shell find src)
RESOURCES := $(shell find resources)
BROWSER_SOURCES := src-cljs/flureedb.cljs
WEBWORKER_SOURCES := src-cljs/flureeworker.cljs
NODEJS_SOURCES := $(shell find src-nodejs)
ALL_SOURCES := $(SOURCES) $(BROWSER_SOURCES) $(WEBWORKER_SOURCES) $(NODEJS_SOURCES)

target/fluree-db.jar: pom.xml out node_modules src/deps.cljs $(ALL_SOURCES) $(RESOURCES)
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

pom.xml: deps.edn
	clojure -Spom

deps:
	clojure -A:cljtest:cljstest -P

src/deps.cljs: package.json
	clojure -M:js-deps

install: target/fluree-db.jar
	clojure -M:install

deploy: target/fluree-db.jar
	clojure -M:deploy

doc/clj/fluree.db.api.html doc/clj/index.html: pom.xml src/fluree/db/api.clj
	clojure -M:docs

doc/clj/%.html: doc/%.md
	clojure -M:docs

docs: doc/clj/fluree.db.api.html doc/clj/index.html $(DOCS_TARGETS)

cljstest: node_modules package-lock.json
	clojure -M:cljstest

cljtest:
	clojure -M:cljtest

test: cljtest cljstest

clean:
	rm -rf target
	rm -rf out
	rm -rf doc/clj/*.html
	rm -rf node_modules
