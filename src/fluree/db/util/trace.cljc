(ns fluree.db.util.trace
  #?(:clj (:require [fluree.db.util :refer [if-cljs]]
                    [steffan-westcott.clj-otel.api.trace.span :as span]
                    [steffan-westcott.clj-otel.api.trace.chan-span :as chan-span]
                    [steffan-westcott.clj-otel.context :as otel-context])))

(defn get-context
  []
  #?(:clj (otel-context/dyn)
     :cljs {}))

#?(:clj
   (defmacro async-with-context
     [id trace-ctx & body]
     (if-cljs
      `(do ~@body)
      `(chan-span/chan-span-binding [_context# {:parent ~trace-ctx :name ~id}]
                                    ~@body))))

#?(:clj
   (defmacro async-form
     [id data & forms]
     (if-cljs
      `(do ~@forms)
      `(chan-span/async-bound-chan-span {:name ~id :attributes ~data}
                                        ~@forms))))
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
