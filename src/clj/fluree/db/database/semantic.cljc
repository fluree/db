(ns fluree.db.database.semantic
  (:require [fluree.db.database :as database :refer [Database]]
            [fluree.db.util.async :refer [<? go-try]]
            [fluree.db.json-ld.iri :as iri]
            [clojure.core.async :as async]
            [#?(:clj clojure.pprint, :cljs cljs.pprint) :as pprint :refer [pprint]])
  #?(:clj (:import (java.io Writer))))

(defrecord SemanticDB [conn alias branch commit t tt-id stats spot post opst tspo novelty
                       schema staged policy namespaces namespace-codes]
  iri/IRICodec
  (encode-iri [_ iri]
    (iri/iri->sid iri namespaces))
  (decode-sid [_ sid]
    (iri/sid->iri sid namespace-codes)))
