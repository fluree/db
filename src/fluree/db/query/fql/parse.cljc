(ns fluree.db.query.fql.parse
  (:require [fluree.db.query.exec :as exec]
            [clojure.spec.alpha :as spec]
            [clojure.string :as str]
            [fluree.json-ld :as json-ld]
            [fluree.db.query.range :as query-range]
            [clojure.core.async :as async :refer [<! >! go go-loop]]
            [fluree.db.flake :as flake]
            [fluree.db.util.async :refer [<? go-try merge-into?]]
            [fluree.db.util.core :as util :refer [try* catch*]]
            [fluree.db.query.analytical-filter :as filter]
            [fluree.db.util.log :as log :include-macros true]
            [fluree.db.dbproto :as dbproto]
            [fluree.db.constants :as const])
  (:import (clojure.lang MapEntry)))

(defn ->pattern
  [typ data]
  (MapEntry/create typ data))

(defn sid?
  [x]
  (int? x))

(def rdf-type-preds #{"http://www.w3.org/1999/02/22-rdf-syntax-ns#type"
                      "a"
                      :a
                      "rdf:type"
                      :rdf/type
                      "@type"})

(defn rdf-type?
  [p]
  (contains? rdf-type-preds p))

(defn variable?
  [x]
  (and (or (string? x) (symbol? x) (keyword? x))
       (-> x name first (= \?))))

(defn query-fn?
  "Query function as positioned in a :where statement"
  [x]
  (and (string? x)
       (re-matches #"^#\(.+\)$" x)))

(def ^:const default-recursion-depth 100)

(defn recursion-predicate
  "A predicate that ends in a '+', or a '+' with some integer afterwards is a recursion
  predicate. e.g.: person/follows+3

  Returns a two-tuple of predicate followed by # of times to recur.

  If not a recursion predicate, returns nil."
  [predicate context]
  (when (or (string? predicate)
            (keyword? predicate))
    (when-let [[_ pred recur-n] (re-find #"(.+)\+(\d+)?$" (name predicate))]
      [(json-ld/expand (keyword (namespace predicate) pred)
                       context)
       (if recur-n
         (util/str->int recur-n)
         default-recursion-depth)])))

(defn parse-variable
  [x]
  (when (variable? x)
    {::exec/var (symbol x)}))

(defn parse-pred-ident
  [x]
  (when (util/pred-ident? x)
    {::exec/ident x}))

(defn parse-subject-id
  ([x]
   (when (sid? x)
     {::exec/val x}))

  ([x context]
   (if-let [parsed (parse-subject-id x)]
     parsed
     (when context
       {::exec/val (json-ld/expand-iri x context)}))))

(defn parse-subject-pattern
  [s-pat context]
  (when s-pat
    (or (parse-variable s-pat)
        (parse-pred-ident s-pat)
        (parse-subject-id s-pat context)
        (throw (ex-info (str "Subject values in where statement must be integer subject IDs or two-tuple identies. "
                             "Provided: " s-pat ".")
                        {:status 400 :error :db/invalid-query})))))

(defn parse-class-predicate
  [x]
  (when (rdf-type? x)
    {::exec/val const/$rdf:type}))

(defn parse-iri-predicate
  [x]
  (when (= "@id" x)
    {::exec/val const/$iri}))

(defn iri->id
  [iri db context]
  (let [full-iri (json-ld/expand-iri iri context)]
    (dbproto/-p-prop db :id full-iri)))

(defn iri->pred-id
  [iri db context]
  (or (iri->id iri db context)
      (throw (ex-info (str "Invalid predicate: " iri)
                      {:status 400 :error :db/invalid-query}))))

(defn parse-recursion-predicate
  [x db context]
  (when-let [[p-iri recur-n] (recursion-predicate x context)]
    {::exec/val   (iri->pred-id p-iri db context)
     ::exec/recur (or recur-n util/max-integer)}))

(defn parse-full-text-predicate
  [x db context]
  (when (and (string? x)
             (str/starts-with? x "fullText:"))
    {::exec/full-text (iri->pred-id (subs x 9) db context)}))

(defn parse-predicate-id
  [x db context]
  {::exec/val (iri->pred-id x db context)})

(defn parse-predicate-pattern
  [p-pat db context]
  (or (parse-iri-predicate p-pat)
      (parse-class-predicate p-pat)
      (parse-variable p-pat)
      (parse-recursion-predicate p-pat db context)
      (parse-full-text-predicate p-pat db context)
      (parse-predicate-id p-pat db context)))

(defn parse-class
  [o-iri db context]
  (if-let [id (iri->id o-iri db context)]
    (parse-subject-id id)
    (throw (ex-info (str "Undefined RDF type specified: " (json-ld/expand-iri o-iri context))
                    {:status 400 :error :db/invalid-query}))))

(defn parse-object-pattern
  [o-pat]
  (or (parse-variable o-pat)
      (parse-pred-ident o-pat)
      {::exec/val o-pat}))

(defmulti parse-pattern
  (fn [pattern db context]
    (if (map? pattern)
      (->> pattern keys first)
      :tuple)))

(defmethod parse-pattern :tuple
  [[s-pat p-pat o-pat] db context]
  (let [s (parse-subject-pattern s-pat context)
        p (parse-predicate-pattern p-pat db context)]
    (if (= const/$rdf:type (::exec/val p))
      (let [cls (parse-class o-pat db context)]
        (->pattern :class [s p cls]))
      (let [o     (parse-object-pattern o-pat)
            tuple [s p o]]
        (if (= const/$iri (::exec/val p))
          (->pattern :iri tuple)
          tuple)))))

(defn parse-where
  [where-clause db context]
  (mapv (fn [pattern]
          (parse-pattern pattern db context))
        where-clause))

(defn parse-context
  [q db]
  (let [db-ctx (get-in db [:schema :context])
        q-ctx  (or (:context q) (get q "@context"))]
    (json-ld/parse-context db-ctx q-ctx)))

(defn parse
  [q db]
  (let [context (parse-context q db)]
    (-> q
        (assoc :context context)
        (update :where parse-where db context))))
