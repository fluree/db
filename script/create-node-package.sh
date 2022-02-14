#!/bin/sh

echo "packaging nodejs..."
mkdir -p out/nodejs/package
# remove shebang from clojurescript compiler output
tail -n +2 out/nodejs/flureenjs.js > out/nodejs/flureenjs.bare.js;
# add UMD wrapper https://groups.google.com/g/clojurescript/c/vNTGZht1XhE
cat script/umd-wrapper.prefix out/nodejs/flureenjs.bare.js script/umd-wrapper.suffix > out/nodejs/package/flureenjs.js;
# add package.json
cp package.json out/nodejs/package/
echo "new flureenjs package at: out/nodejs/package/"
