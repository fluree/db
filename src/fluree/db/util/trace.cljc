(ns fluree.db.util.trace
  #?(:clj (:require [fluree.db.util :refer [if-cljs]]
                    [steffan-westcott.clj-otel.api.trace.span :as span])))

#?(:clj
   (defmacro form
     [id data & forms]
     (if-cljs
      `(do ~@forms)
      `(span/with-span! {:name ~id :attributes ~data}
         ~@forms))))

#?(:clj
   (defmacro xf
     [id data]
     (if-cljs
      `(do ~id)
      `(fn [rf#]
         (let [logged?# (volatile! false)]
           (fn
             ([] (rf#))
             ([result# x#]
              (when-not @logged?#
                (form ~id ~data)
                (vreset! logged?# true))
              (rf# result# x#))
             ([result#] (rf# result#))))))))
