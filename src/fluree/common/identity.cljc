(ns fluree.common.identity
  (:require [fluree.crypto :as crypto]
            [clojure.string :as str]))

(defn create-id
  "An id uniquely identifies a subject, regardless of where that subject lives."
  [type input]
  (str "fluree:" (name type) ":" (crypto/sha2-256 input)))

(defn id-parts
  "Returns the components of an id: the :id/ns, :id/type, and :id/hash."
  [id]
  (let [[ns type hash] (str/split id #":")]
    {:id/ns ns
     :id/type (keyword type)
     :id/hash hash}))

(defn create-address
  "An address reveals how to access the subject it addresses, by answering 1) what type of
  subject it is (so the resolver can be determined), 2) how to access it (the method),
  and 3) where it is (the path)."
  [class method path]
  (str "fluree:" (name class) ":" (name method) ":" path))

(defn address-parts
  [address]
  (let [[ns type method path] (str/split address #":")
        [ledger-name _ id]    (str/split path #"/")]
    {:address/ns     ns
     :address/type   (keyword type)
     :address/method (keyword method)
     :address/ledger-name ledger-name
     :address/path   path
     :address/id     id}))
