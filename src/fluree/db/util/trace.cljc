(ns fluree.db.util.trace
  #?(:clj (:require [fluree.db.util :refer [if-cljs]]
                    [steffan-westcott.clj-otel.api.trace.span :as span]
                    [steffan-westcott.clj-otel.context :as otel-context])))

(defn get-context
  []
  #?(:clj (otel-context/dyn)
     :cljs {}))

#?(:clj
   (defmacro with-context
     [id trace-ctx & body]
     `(span/with-span! {:name ~id :parent ~trace-ctx}
        ~@body)))

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
