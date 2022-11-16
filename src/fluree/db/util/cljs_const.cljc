(ns fluree.db.util.cljs-const
  (:refer-clojure :exclude [case])
  #?(:cljs (:require-macros [fluree.db.util.cljs-const])))

#?(:clj (set! *warn-on-reflection* true))

#?(:clj
   (defmacro case
     "This is the CLJS version of const/case that doesn't need to do anything
     because in CLJS ^:const values are inlined by the compiler.

     NB: Make sure you are only using dispatch values compatible with
     cljs.core/case"
     [value & clauses]
     `(clojure.core/case ~value
        ~@clauses)))
