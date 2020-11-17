.PHONY: deps jar install deploy nodejs browser clean

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

package-lock.json node_modules: package.json
	npm install && touch package-lock.json node_modules

out/flureenjs.js: out package.json package-lock.json node_modules build-nodejs.edn deps.edn src/deps.cljs $(SOURCES) $(NODEJS_SOURCES) $(RESOURCES)
	clojure -M:nodejs && cp out/nodejs/flureenjs.js out/flureenjs.js

nodejs: out/flureenjs.js

out/flureedb.js: out package.json package-lock.json node_modules build-browser.edn deps.edn src/deps.cljs $(SOURCES) $(BROWSER_SOURCES) $(RESOURCES)
	clojure -M:browser && cp out/browser/main.js out/flureedb.js

browser: out/flureedb.js


pom.xml: deps.edn
	clojure -Spom

deps:
	clojure -P

src/deps.cljs: package.json
	clojure -M:js-deps

install: target/fluree-db.jar
	clojure -M:install

deploy: target/fluree-db.jar
	clojure -M:deploy

clean:
	rm -rf target
	rm -rf out
	rm -rf node_modules
