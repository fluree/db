(ns fluree.db.util.schema
  (:require [fluree.db.flake :as flake]
            [fluree.db.constants :as const]))

#?(:clj (set! *warn-on-reflection* true))


(def ^:const schema-sid-start (flake/min-subject-id const/$_predicate))
(def ^:const schema-sid-end (flake/max-subject-id const/$_collection))

(def ^:const collection-sid-start (flake/min-subject-id const/$_collection))
(def ^:const collection-sid-end (flake/max-subject-id const/$_collection))

(def ^:const predicate-sid-start (flake/min-subject-id const/$_predicate))
(def ^:const predicate-sid-end (flake/max-subject-id const/$_predicate))

(def ^:const setting-sid-start (flake/min-subject-id const/$_setting))
(def ^:const setting-sid-end (flake/max-subject-id const/$_setting))

(def ^:const auth-sid-start (flake/min-subject-id const/$_auth))
(def ^:const auth-sid-end (flake/max-subject-id const/$_auth))

(def ^:const role-sid-start (flake/min-subject-id const/$_role))
(def ^:const role-sid-end (flake/max-subject-id const/$_role))

(def ^:const rule-sid-start (flake/min-subject-id const/$_rule))
(def ^:const rule-sid-end (flake/max-subject-id const/$_rule))

(def ^:const fn-sid-start (flake/min-subject-id const/$_fn))
(def ^:const fn-sid-end (flake/max-subject-id const/$_fn))

(def ^:const tag-sid-start (flake/min-subject-id const/$_tag))
(def ^:const tag-sid-end (flake/max-subject-id const/$_tag))

(def ^:const sys-collection-end (flake/max-subject-id const/$numSystemCollections))

(defn is-tx-meta-flake?
  "Returns true if this flake is for tx-meta"
  [f]
  (< (flake/s f) 0))

(defn is-schema-cid?
  "Returns true if collection id is a schema flake."
  [cid]
  (or (= cid const/$_predicate)
      (= cid const/$_collection)))

(defn is-schema-flake?
  "Returns true if flake is a schema flake."
  [f]
  (<= schema-sid-start (flake/s f) schema-sid-end))

(defn is-system-flake?
  "Returns true if flake is a in a system collection."
  [f]
  (<= 0 (flake/s f) sys-collection-end))

(defn is-setting-flake?
  "Returns true if flake is a root setting flake."
  [f]
  (<= setting-sid-start (flake/s f) setting-sid-end))

(defn is-language-flake?
  "Returns true if flake is a language flake."
  [f]
  (= (flake/p f) const/$_setting:language))

(defn is-genesis-flake?
  "Returns true if flake is a root setting flake."
  [f]
  (cond
    (<= tag-sid-start (flake/s f) tag-sid-end) true
    (is-setting-flake? f) true
    (<= auth-sid-start (flake/s f) auth-sid-end) true
    (<= role-sid-start (flake/s f) role-sid-end) true
    (<= rule-sid-start (flake/s f) rule-sid-end) true
    (<= fn-sid-start (flake/s f) fn-sid-end) true
    (and (<= collection-sid-start (flake/s f) collection-sid-end)
         (<= (flake/sid->i (flake/s f)) const/$numSystemCollections)) true
    (and (<= predicate-sid-start (flake/s f) predicate-sid-end)
         (<= (flake/sid->i (flake/s f)) const/$maxSystemPredicates)) true

    :else false))

(defn add-to-post-preds?
  [flakes pred-ecount]
  (keep #(let [f %]
           (if (and (or (= (flake/p f) const/$_predicate:index)
                        (= (flake/p f) const/$_predicate:unique))
                  (= (flake/o f) true)
                  (>= pred-ecount (flake/s f))) (flake/s f)))
        flakes))

(defn remove-from-post-preds
  "Returns any predicate subject flakes that are removing
  an existing index, either via index: true or unique: true."
  [flakes]
  (keep #(let [f %]
           (when (and (true? (flake/op f))
                    (or (= (flake/p f) const/$_predicate:index)
                        (= (flake/p f) const/$_predicate:unique))
                    (= (flake/o f) false)) (flake/s f)))
        flakes))

(defn schema-change?
  "Returns true if any of the provided flakes are a schema flake."
  [flakes]
  (some is-schema-flake? flakes))

(defn setting-change?
  [flakes]
  (some is-setting-flake? flakes))

(defn system-change?
  "Returns true if any of the provided flakes are in any system collection."
  [flakes]
  (some is-system-flake? flakes))


(defn get-language-change
  "Returns the language being added, if any. Else returns nil."
  [flakes]
  (some #(let [f %]
           (when (and (is-language-flake? f)
                    (is-setting-flake? f)
                    (true? (flake/op f))) (flake/o f)))
        flakes))

(defn is-pred-flake?
  "Returns true if flake is a schema flake."
  [f]
  (<= flake/MIN-PREDICATE-ID (flake/s f) flake/MAX-PREDICATE-ID))


(defn pred-change?
  "Returns true if there are any predicate changes present in set of flakes."
  [flakes]
  (some is-pred-flake? flakes))
