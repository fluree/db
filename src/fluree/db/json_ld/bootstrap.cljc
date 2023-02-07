(ns fluree.db.json-ld.bootstrap
  (:require [clojure.string :as str]
            [clojure.core.async :refer [go]]
            [fluree.crypto :as crypto]
            [fluree.db.ledger.proto :as ledger-proto]
            [fluree.db.util.core :as util #?(:clj :refer :cljs :refer-macros) [try* catch*]]
            [fluree.db.util.json :as json]
            [fluree.json-ld :as json-ld]
            [fluree.db.dbproto :as db-proto]
            [fluree.db.json-ld.transact :as jld-transact]
            [fluree.db.flake :as flake]
            [fluree.db.util.log :as log :include-macros true]))

#?(:clj (set! *warn-on-reflection* true))

(def root-role-id "fluree-root-role")

(def root-role
  {"@id"             root-role-id
   "@type"           ["Role"]
   "skos:definition" "Default role that gives full root access to a ledger."
   "skos:prefLabel"  "Root role"
   "rules"           ["fluree-root-rule"]})

(def root-rule
  {"@id"             "fluree-root-rule"
   "@type"           ["Rule"]
   "skos:definition" "Default root rule, attached to fluree-root-role."
   "skos:prefLabel"  "Root rule"
   "allTypes"        true
   "functions"       ["fluree-fn-true"]
   "operations"      ["opsAll"]})


(def true-fn
  {"@id"             "fluree-fn-true"
   "@type"           ["Function"]
   "skos:definition" "Always allows full access to any data when attached to a rule."
   "skos:prefLabel"  "True function"
   "code"            "true"})

(def false-fn
  {"@id"             "fluree-fn-false"
   "@type"           ["Function"]
   "skos:definition" "Always denies access to any data when attached to a rule."
   "skos:prefLabel"  "False function"
   "code"            "false"})

(def default-tx
  [root-role
   root-rule
   true-fn
   false-fn])

(defn fluree-account-id?
  "A fluree account id (_auth/id) starts with 'T' and is 35 characters long."
  [x]
  (and (string? x)
       (= \T (first x))
       (= 35 (count x))))




(defn public-key?
  "Returns account id if valid public key, else nil."
  [x]
  (and (string? x)
       (try* (crypto/account-id-from-public x)
             true
             (catch* _ false))))


(defn dids-tx
  "Adds decentralized identifiers to the bootstrap transaction"
  [dids]
  (let [dids    (util/sequential dids)
        base-tx {"@id"   nil
                 "@type" ["DID"]
                 "role"  root-role-id}]
    (mapv
      (fn [did]
        (cond
          ;; already in a did format, just assign to root role
          (str/starts-with? did "did:")
          (assoc base-tx "@id" did)

          (fluree-account-id? did)
          (assoc base-tx "@id" (str "did:fluree:" did))

          (public-key? did)
          (let [account-id (crypto/account-id-from-public did)]
            (assoc base-tx "@id" (str "did:fluree:" account-id)))

          :else
          (throw (ex-info (str "Invalid auth/did provided when bootstrapping db: " did)
                          {:status 400 :error :db/invalid-auth}))))
      dids)))

(defn ctx-url?
  "Returns if a single URL-based context like: https://schema.org"
  [default-context]
  (or (str/starts-with? default-context "https://")
      (str/starts-with? default-context "http://")))

(defn normalize-default-ctx
  "Default context should be stored as a JSON data structure.
  This verifies it is valid JSON."
  [default-ctx]
  (cond
    (and (string? default-ctx)
         (ctx-url? default-ctx))
    (json/stringify default-ctx)

    (string? default-ctx)
    (try* (-> default-ctx
              json/parse
              json-ld/parse-context)
          ;; if no errors from above operation, appears to be valid, return original JSON.
          default-ctx
          (catch* _ (throw (ex-info (str "Invalid default context provided bootstrapping ledger. Must be valid JSON: " default-ctx)
                                    {:status 400 :error :db/invalid-context}))))

    (or (map? default-ctx)
        (sequential? default-ctx))
    (try*
      (json-ld/parse-context default-ctx)
      ;; if no errors from above operation, appears to be valid, return JSON.
      (json/stringify default-ctx)
      (catch* _ (throw (ex-info (str "Invalid default context provided bootstrapping ledger. Must be a valid context: " default-ctx)
                                {:status 400 :error :db/invalid-context}))))

    :else
    (throw (ex-info (str "Invalid default context provided bootstrapping ledger. "
                         "Must be a valid JSON context, or a valid context map or array/vector. Provided: " default-ctx)
                    {:status 400 :error :db/invalid-context}))))

(defn bootstrap
  "Bootstraps a permissioned JSON-LD db. Returns async channel."
  ([blank-db] (bootstrap blank-db nil))
  ([blank-db initial-tx]
   (if-let [tx (when initial-tx
                 {"@context" "https://ns.flur.ee/ledger/v1"
                  "@graph"   initial-tx})]
     (db-proto/-stage blank-db tx {:bootstrap? true})
     (go blank-db))))

(defn blank-db
  "When not bootstrapping with a transaction, bootstraps initial base set of flakes required for a db."
  [blank-db]
  (let [t           -1
        base-flakes (jld-transact/base-flakes t)
        size (flake/size-bytes base-flakes)]
    (-> blank-db
        (update-in [:novelty :spot] into base-flakes)
        (update-in [:novelty :psot] into base-flakes)
        (update-in [:novelty :post] into base-flakes)
        (update-in [:novelty :tspo] into base-flakes)
        (update-in [:novelty :size] + size)
        (update-in [:stats :size] + size)
        (update-in [:stats :flakes] + (count base-flakes)))))
