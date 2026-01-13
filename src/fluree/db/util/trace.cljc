(ns fluree.db.util.trace
  #?(:clj
     (:require [fluree.db.util :refer [if-cljs]]
               [steffan-westcott.clj-otel.api.trace.chan-span :as chan-span]
               [steffan-westcott.clj-otel.api.trace.span :as span]
               [steffan-westcott.clj-otel.context :as otel-context])))

(defn get-context
  "Returns the current tracing context."
  []
  #?(:clj (otel-context/dyn)
     :cljs nil))

#?(:clj
   (defmacro with-parent-context
     "Start a new tracing span as a child of the supplied parent trace context."
     [id parent-trace-ctx & body]
     `(if-cljs
       (do ~@body)
       (chan-span/chan-span-binding [_context# {:parent ~parent-trace-ctx :name ~id}]
                                    ~@body))))

#?(:clj
   (defmacro async-form
     "Trace async body execution using the dynamically bound tracing context."
     [id data & body]
     `(if-cljs
       (do ~@body)
       (chan-span/async-bound-chan-span {:name ~id :attributes ~data}
                                        ~@body))))

#?(:clj
   (defmacro form
     "Trace body execution using the dynamically bound tracing context or the supplied :parent context."
     [id opts & body]
     `(if-cljs
       (do ~@body)
       (span/with-span! ~(merge {:name id} opts)
         ~@body))))

#?(:clj
   (defmacro xf
     "Trace the first time a transducer is executed."
     [id data]
     `(fn [rf#]
        (let [logged?# (volatile! false)]
          (fn
            ([] (rf#))
            ([result# x#]
             (when-not @logged?#
               (form ~id {:attributes ~data})
               (vreset! logged?# true))
             (rf# result# x#))
            ([result#] (rf# result#)))))))

