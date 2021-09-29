#!/bin/sh

set -e

# strip shebang
tail -n +2 index.js > bare.js
mv bare.js index.js

# add UMD wrapper https://groups.google.com/g/clojurescript/c/vNTGZht1XhE
cat umd-wrapper.prefix index.js umd-wrapper.suffix > umd.js
mv umd.js index.js
