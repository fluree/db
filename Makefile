.PHONY: all deps jar install deploy nodejs browser webworker cljtest	\
	cljs-browser-test cljs-node-test cljstest test eastwood ci clean	\
	js-packages sync-package-json publish-nodejs publish-browser		\
	publish-webworker publish-js pending-tests pt

DOCS_MARKDOWN := $(shell find docs -name '*.md')
DOCS_TARGETS := $(DOCS_MARKDOWN:docs/%.md=docs/%.html)

SOURCES := $(shell find src)
RESOURCES := $(shell find resources)
BROWSER_SOURCES := src/fluree/sdk/browser.cljs
NODEJS_SOURCES := src/fluree/sdk/node.cljs
WEBWORKER_SOURCES := src/fluree/sdk/webworker.cljs
ALL_SOURCES := $(SOURCES) $(BROWSER_SOURCES) $(WEBWORKER_SOURCES) $(NODEJS_SOURCES)

all: jar browser nodejs webworker js-packages docs

target/fluree-db.jar: out node_modules src/deps.cljs $(ALL_SOURCES) $(RESOURCES)
	clojure -T:build jar

jar: target/fluree-db.jar

package-lock.json node_modules: package.json
	npm install && touch package-lock.json node_modules

out/fluree-node-sdk.js: package.json package-lock.json node_modules deps.edn src/deps.cljs shadow-cljs.edn $(SOURCES) $(NODEJS_SOURCES) $(RESOURCES)
	npx shadow-cljs release fluree-node-sdk && cp out/nodejs/fluree-node-sdk.js out/fluree-node-sdk.js

nodejs: out/fluree-node-sdk.js

out/fluree-browser-sdk.js: package.json package-lock.json node_modules deps.edn src/deps.cljs shadow-cljs.edn $(SOURCES) $(BROWSER_SOURCES) $(RESOURCES)
	npx shadow-cljs release fluree-browser-sdk && cp out/browser/fluree-browser-sdk.js out/fluree-browser-sdk.js

browser: out/fluree-browser-sdk.js

out/fluree-webworker.js: package.json package-lock.json node_modules deps.edn src/deps.cljs shadow-cljs.edn $(SOURCES) $(WEBWORKER_SOURCES) $(RESOURCES)
	npx shadow-cljs release fluree-webworker && cp out/webworker/fluree-webworker.js out/fluree-webworker.js

webworker: out/fluree-webworker.js

deps:
	clojure -A:cljtest:cljstest:eastwood:docs -P

src/deps.cljs: package.json
	clojure -M:js-deps

install: target/fluree-db.jar
	clojure -T:build install

deploy: target/fluree-db.jar
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

js-packages: sync-package-json js-packages/nodejs/fluree-node-sdk.js js-packages/browser/fluree-browser-sdk.js js-packages/webworker/fluree-webworker.js

sync-package-json: js-packages/nodejs/package.json js-packages/browser/package.json js-packages/webworker/package.json

NPM_TAG ?= latest

publish-nodejs: js-packages/nodejs/fluree-node-sdk.js js-packages/nodejs/package.json
	cd $(<D) && npm publish --tag $(NPM_TAG)

publish-browser: js-packages/browser/fluree-browser-sdk.js js-packages/browser/package.json
	cd $(<D) && npm publish --tag $(NPM_TAG)

publish-webworker: js-packages/webworker/fluree-webworker.js js-packages/webworker/package.json
	cd $(<D) && npm publish --tag $(NPM_TAG)

publish-js: publish-nodejs publish-browser publish-webworker

docs/fluree.db.json-ld.api.html docs/index.html: src/fluree/db/json_ld/api.cljc
	clojure -T:build docs :output-path "\"$(@D)\""

docs/%.html: docs/%.md
	clojure -T:build docs :output-path "\"$(@D)\""

docs: docs/fluree.db.json-ld.api.html docs/index.html $(DOCS_TARGETS)

cljs-browser-test: node_modules package-lock.json
	npx shadow-cljs release browser-test
	./node_modules/karma/bin/karma start --single-run

cljs-node-test: node_modules package-lock.json
	npx shadow-cljs release node-test

nodejs-test: out/fluree-node-sdk.js
	cd test/nodejs && npm install && node --experimental-vm-modules node_modules/jest/bin/jest.js

browser-test: out/fluree-browser-sdk.js
	cd test/browser && npm install && CI=true npm test

cljstest: cljs-browser-test cljs-node-test

cljtest:
	clojure -X:dev:cljtest

pending-tests:
	clojure -X:dev:pending-tests

pt: pending-tests

test: cljtest cljstest nodejs-test browser-test

eastwood:
	clojure -M:dev:cljtest:eastwood

ci: test eastwood

clean:
	clojure -T:build clean
	rm -rf out/*
	rm -rf docs/*.html
	rm -rf node_modules
	rm -rf test/nodejs/store
	rm -rf .shadow-cljs
	rm -rf js-packages/browser/fluree-browser-sdk.js
	rm -rf js-packages/nodejs/fluree-node-sdk.js
	rm -rf js-packages/webworker/fluree-webworker.js
