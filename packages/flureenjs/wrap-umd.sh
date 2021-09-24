#!/bin/sh

# add UMD wrapper https://groups.google.com/g/clojurescript/c/vNTGZht1XhE

cat ./umd-wrapper.prefix ./flureenjs.bare.js ./umd-wrapper.suffix > ./flureenjs.js;
# make sure we've got the correct deps specified
#bb release-js/release_node.clj
