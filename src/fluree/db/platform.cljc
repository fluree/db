(ns fluree.db.platform)

#?(:cljs
   (goog-define BROWSER true)
   :clj (def BROWSER false))
