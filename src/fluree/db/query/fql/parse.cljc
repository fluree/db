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
            [fluree.db.constants :as const]))

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

(defn pred-id-strict
  "Returns predicate ID for a given predicate, else will throw with an invalid
  predicate error."
  [db predicate]
  (or (dbproto/-p-prop db :id predicate)
      (throw (ex-info (str "Invalid predicate: " predicate)
                      {:status 400 :error :db/invalid-query}))))

(defn parse-subject
  [s context]
  (cond
    (util/pred-ident? s)
    {::exec/ident s}

    (variable? s)
    {::exec/var (symbol s)}

    (nil? s)
    nil

    context
    {::exec/val (if (int? s)
                  s
                  (json-ld/expand-iri s context))}

    :else
    (if (not (int? s))
      (throw (ex-info (str "Subject values in where statement must be integer subject IDs or two-tuple identies. "
                           "Provided: " s ".")
                      {:status 400 :error :db/invalid-query}))
      {::exec/val s})))

(defn parse-predicate
  [p db context]
  (cond
    (rdf-type? p)
    {::exec/val const/$rdf:type}

    (= "@id" p)
    {::exec/val const/$iri}

    (variable? p)
    {::exec/var (symbol p)}

    (recursion-predicate p context)
    (let [[p-iri recur-n] (recursion-predicate p context)]
      {::exec/val (pred-id-strict db p-iri)
       ::exec/recur (or recur-n util/max-integer)}) ;; default recursion depth

    (and (string? p)
         (str/starts-with? p "fullText:"))
    {::exec/full-text (->> (json-ld/expand-iri (subs p 9) context)
                           (pred-id-strict db))}

    :else
    {::exec/val (->> (json-ld/expand-iri p context)
                     (pred-id-strict db))}))

(defn parse-object
  [o context]
  (cond
    (util/pred-ident? o)
    {::exec/ident o}

    (variable? o)
    {::exec/var (symbol o)}

    (nil? o)
    nil

    context
    {::exec/val (if (int? o)
                  o
                  (json-ld/expand-iri o context))}

    :else
    {::exec/val o}))

(defn parse-tuple
  [[s p o] db context]
  [(parse-subject s context)
   (parse-predicate p db context)
   (parse-object o context)])

(defn parse-where
  [where-clause db context]
  (mapv (fn [pattern]
          (parse-tuple pattern db context))
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
