(ns fluree.db.util.schema
  (:require [fluree.db.flake :as flake #?@(:cljs [:refer [Flake]])]
            [fluree.db.constants :as const])
  #?(:clj (:import (fluree.db.flake Flake))))

#?(:clj (set! *warn-on-reflection* true))


(def ^:const schema-sid-start (flake/min-subject-id const/$_predicate))
(def ^:const schema-sid-end (flake/max-subject-id const/$_collection))

(def ^:const prefix-sid-start (flake/min-subject-id const/$_prefix))
(def ^:const prefix-sid-end (flake/max-subject-id const/$_prefix))

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

(def ^:const pred-reverse-ref-re #"(?:([^/]+)/)_([^/]+)")

(defn reverse-ref?
  "Reverse refs must be strings that include a '/_' in them, which characters before and after."
  [predicate-name throw?]
  (if (string? predicate-name)
    (boolean (re-matches pred-reverse-ref-re predicate-name))
    (if throw?
      (throw (ex-info (str "Bad predicate name, should be string: " (pr-str predicate-name))
                      {:status 400
                       :error  :db/invalid-predicate}))
      false)))

(defn is-tx-meta-flake?
  "Returns true if this flake is for tx-meta"
  [^Flake f]
  (< (.-s f) 0))

(defn is-schema-sid?
  "Returns true if subject id is that of a schema element."
  [sid]
  (or
    (<= schema-sid-start sid schema-sid-end)
    (<= prefix-sid-start sid prefix-sid-end)))

(defn is-schema-flake?
  "Returns true if flake is a schema flake."
  [^Flake f]
  (is-schema-sid? (.-s f)))

(defn is-setting-sid?
  "Returns true if sid is for a root setting."
  [sid]
  (<= setting-sid-start sid setting-sid-end))

(defn is-setting-flake?
  "Returns true if flake is a root setting flake."
  [^Flake f]
  (is-setting-sid? (.-s f)))

(defn is-language-flake?
  "Returns true if flake is a language flake."
  [^Flake f]
  (= (.-p f) const/$_setting:language))

(defn is-genesis-flake?
  "Returns true if flake is a root setting flake."
  [^Flake f]
  (cond
    (and (<= tag-sid-start (.-s f) tag-sid-end)) true
    (is-setting-flake? f) true
    (<= auth-sid-start (.-s f) auth-sid-end) true
    (<= role-sid-start (.-s f) role-sid-end) true
    (<= rule-sid-start (.-s f) rule-sid-end) true
    (<= fn-sid-start (.-s f) fn-sid-end) true
    (and (<= collection-sid-start (.-s f) collection-sid-end)
         (<= (flake/sid->i (.-s f)) const/$numSystemCollections)) true
    (and (<= predicate-sid-start (.-s f) predicate-sid-end)
         (<= (flake/sid->i (.-s f)) const/$maxSystemPredicates)) true

    :else false))

(defn add-to-post-preds?
  [flakes pred-ecount]
  (keep #(let [f ^Flake %]
           (when (and (or (= (.-p f) const/$_predicate:index)
                          (= (.-p f) const/$_predicate:unique))
                      (= (.-o f) true)
                      (>= pred-ecount (.-s f)))
             (.-s f)))
        flakes))

(defn remove-from-post-preds
  "Returns any predicate subject flakes that are removing
  an existing index, either via index: true or unique: true."
  [flakes]
  (keep #(let [f ^Flake %]
           (when (and (true? (.-op f))
                    (or (= (.-p f) const/$_predicate:index)
                        (= (.-p f) const/$_predicate:unique))
                    (= (.-o f) false)) (.-s f)))
        flakes))

(defn schema-change?
  "Returns true if any of the provided flakes are a schema flake."
  [flakes]
  (some is-schema-flake? flakes))

(defn setting-change?
  [flakes]
  (some is-setting-flake? flakes))


(defn get-language-change
  "Returns the language being added, if any. Else returns nil."
  [flakes]
  (some #(let [f ^Flake %]
           (when (and (is-language-flake? f)
                    (is-setting-flake? f)
                    (true? (.-op f))) (.-o f)))
        flakes))

(defn is-pred-flake?
  "Returns true if flake is a schema flake."
  [^Flake f]
  (<= flake/MIN-PREDICATE-ID (.-s f) flake/MAX-PREDICATE-ID))


(defn pred-change?
  "Returns true if there are any predicate changes present in set of flakes."
  [flakes]
  (some is-pred-flake? flakes))

(defn version
  "Returns schema version from a db, which is the :t when the schema was last updated."
  [db]
  (get-in db [:schema :t]))