.PHONY: deps jar install deploy nodejs browser clean

target/fluree-db.jar: pom.xml out src/deps.cljs src/**/* resources/**/*
	clojure -A:jar

jar: target/fluree-db.jar

out:
	mkdir out

out/nodejs: out
	mkdir out/nodejs

out/nodejs/flureenjs.js: package.json package-lock.json build-nodejs.edn deps.edn out/nodejs src/**/* src-nodejs/**/* resources/**/*
	clojure -A:nodejs

nodejs: out/nodejs/flureenjs.js

out/browser: out
	mkdir out/browser

out/browser/flureedb.js: package.json package-lock.json build-browser.edn deps.edn out/browser src/**/* src-cljs/**/* resources/**/*
	clojure -A:browser

browser: out/browser/flureedb.js

pom.xml: deps.edn
	clojure -Spom

deps:
	clojure -Stree

src/deps.cljs: package.json
	clojure -A:js-deps

install: target/fluree-db.jar
	clojure -A:install

deploy: target/fluree-db.jar
	script/deploy-jar.sh

clean:
	rm -rf target
	rm -rf out
