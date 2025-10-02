(ns fluree.db.util.cbor
  #?(:clj (:import (com.fasterxml.jackson.dataformat.cbor CBORFactory)
                   (com.fasterxml.jackson.databind ObjectMapper)
                   (java.io ByteArrayOutputStream ByteArrayInputStream))))

#?(:clj
   (def ^:private ^ObjectMapper mapper
     (ObjectMapper. (CBORFactory.))))

#?(:clj
   (defn- clj->java
     [x]
     (cond
       (map? x) (let [m (java.util.LinkedHashMap.)]
                  (doseq [[k v] x]
                    (.put m (if (keyword? k) (name k) k) (clj->java v)))
                  m)
       (vector? x) (java.util.ArrayList. ^java.util.Collection (map clj->java x))
       (sequential? x) (java.util.ArrayList. ^java.util.Collection (map clj->java x))
       (keyword? x) (name x)
       :else x)))

#?(:clj
   (defn- java->clj*
     [x]
     (cond
       (instance? java.util.Map x)
       (into {} (for [^java.util.Map$Entry e (.entrySet ^java.util.Map x)]
                  [(keyword (.getKey e)) (java->clj* (.getValue e))]))

       (instance? java.util.List x)
       (vec (map java->clj* ^java.util.List x))

       :else x)))

#?(:cljs
   (def ^:private cborjs
     (try
       (js/require "cbor")
       (catch :default _ nil))))

#?(:clj  (def cbor-available? true)
   :cljs (def cbor-available? (boolean cborjs)))

#?(:cljs
   (defn- js-deep->clj
     [x]
     (cond
       (instance? js/Map x)
       (into {}
             (for [entry (array-seq (js/Array.from (.entries x)))]
               (let [k (aget entry 0)
                     v (aget entry 1)]
                 [(keyword (str k)) (js-deep->clj v)])))

       (instance? js/Array x)
       (vec (map js-deep->clj (array-seq x)))

       (and (some? x) (identical? (type x) js/Object))
       (let [o (js->clj x :keywordize-keys true)]
         (if (map? o)
           (into {} (for [[k v] o] [k (js-deep->clj v)]))
           o))

       :else x)))

(defn encode
  "Encode Clojure data to CBOR bytes."
  [_data]
  #?(:clj
     (let [baos (ByteArrayOutputStream.)]
       (.writeValue mapper baos (clj->java _data))
       (.toByteArray baos))
     :cljs
     (if cborjs
       (.encode cborjs (clj->js _data))
       (throw (ex-info "CBOR encode not available in this CLJS runtime" {})))))

(defn decode
  "Decode CBOR bytes to Clojure data with keyword keys."
  [^bytes _bs]
  #?(:clj
     (let [bais (ByteArrayInputStream. _bs)
           obj  (.readValue mapper bais Object)]
       (java->clj* obj))
     :cljs
     (if cborjs
       (js-deep->clj (.decode cborjs _bs))
       (throw (ex-info "CBOR decode not available in this CLJS runtime" {})))))
