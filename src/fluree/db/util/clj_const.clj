(ns fluree.db.util.clj-const
  (:require [fluree.db.util :refer [case+]])
  (:refer-clojure :exclude [case]))

(set! *warn-on-reflection* true)

(defmacro case
  "Same as case but inlines ^:const values like in CLJS. There is a (no-op)
  CLJS version of this macro too so that you can do this in CLJC code:

  (:require #?(:clj  [fluree.db.util.clj-const :as uc]
               :cljs [fluree.db.util.cljs-const :as uc]))

  (def ^:const thingy1 1)
  (def ^:const thingy2 2)
  (def ^:const thingy3 3)

  (uc/case val
    thingy1 :got-thingy1
    thingy2 :got-thingy2
    thingy3 :got-thingy3
    :got-something-else)

  NB: While this can inline anything that can be eval'd at compile time, you
  should use fluree.db.util/case+ directly if you're using anything but
  ^:const dispatch values and literals."

  [value & clauses]
  `(case+ ~value ~@clauses))
