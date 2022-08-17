(ns fluree.db.conn.platform)

#?(:cljs
   (goog-define BROWSER true)
   :clj (def BROWSER false))
