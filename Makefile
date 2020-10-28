.PHONY: deps test jar install deploy nodejs browser clean docs

DOCS_MARKDOWN := $(shell find doc -name '*.md')
DOCS_TARGETS := $(DOCS_MARKDOWN:doc/%.md=doc/clj/%.html)

SOURCES := $(shell find src)
RESOURCES := $(shell find resources)
BROWSER_SOURCES := src-cljs/flureedb.cljs
WEBWORKER_SOURCES := src-cljs/flureeworker.cljs
NODEJS_SOURCES := $(shell find src-nodejs)
ALL_SOURCES := $(SOURCES) $(BROWSER_SOURCES) $(WEBWORKER_SOURCES) $(NODEJS_SOURCES)

target/fluree-db.jar: pom.xml out node_modules src/deps.cljs $(ALL_SOURCES) $(RESOURCES)
	clojure -M:jar

jar: target/fluree-db.jar

out:
	mkdir out

out/nodejs: out
	mkdir out/nodejs

package-lock.json node_modules: package.json
	npm install && touch package-lock.json node_modules

out/nodejs/flureenjs.js: package.json package-lock.json node_modules build-nodejs.edn deps.edn out/nodejs src/deps.cljs $(SOURCES) $(NODEJS_SOURCES) $(RESOURCES)
	clojure -M:nodejs

nodejs: out/nodejs/flureenjs.js

out/browser: out
	mkdir out/browser

out/browser/flureedb.js: package.json package-lock.json node_modules build-browser.edn deps.edn out/browser src/deps.cljs $(SOURCES) $(BROWSER_SOURCES) $(RESOURCES)
	clojure -M:browser

browser: out/browser/flureedb.js

pom.xml: deps.edn
	clojure -Spom

deps:
	clojure -Stree

src/deps.cljs: package.json
	clojure -M:js-deps

test:
	clojure -M:test

install: target/fluree-db.jar
	clojure -M:install

deploy: target/fluree-db.jar
	clojure -M:deploy

doc/clj/fluree.db.api.html doc/clj/index.html: pom.xml src/fluree/db/api.clj
	clojure -M:docs

doc/clj/%.html: doc/%.md
	clojure -M:docs

docs: doc/clj/fluree.db.api.html doc/clj/index.html $(DOCS_TARGETS)

clean:
	rm -rf target
	rm -rf out
	rm -rf doc/clj/*.html
	rm -rf node_modules
