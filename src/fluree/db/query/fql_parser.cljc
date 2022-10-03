(ns fluree.db.query.fql-parser
  (:require [fluree.db.util.core :as util :refer [try* catch*]]
            [fluree.db.dbproto :as dbproto]
            [clojure.string :as str]
            [fluree.db.spec :as spec]
            [fluree.db.util.async :refer [<? go-try]]
            [fluree.db.query.schema :as schema]))

#?(:clj (set! *warn-on-reflection* true))

(defn- to-select-map
  "Takes a sequential select statement and turns it into a map."
  [select]
  (reduce
    (fn [acc x]
      (let [kv (cond
                 (sequential? x) [(util/keyword->str (first x))
                                  (as-> (second x) v*
                                        (if (coll? v*) (to-select-map v*) v*))]

                 ;; must be a map within a sequence...
                 (map? x)
                 (let [values (mapv (fn [x-item]
                                      (let [key-x  (-> x-item
                                                       key
                                                       util/keyword->str)
                                            val-x  (val x-item)
                                            val-x' (cond
                                                     (= "_orderBy" key-x)
                                                     (if (coll? val-x)
                                                       {:order     (first val-x)
                                                        :predicate (second val-x)}
                                                       {:order     "ASC"
                                                        :predicate val-x})

                                                     (coll? val-x)
                                                     (to-select-map val-x)

                                                     :else val-x)]
                                        [key-x val-x'])) x)]
                   (into [] (flatten values)))

                 :else [x nil])]

        (apply assoc acc kv)))
    {} select))

;; TODO - check :limit below and default setting
(defn parse
  "Parses select statement into our own select format.
  Has no dependency on a database, or a given schema
  at this point.

  Select spec has the following keys:
  :name  - name of the predicate
  :namespace? - if the predicate 'name' has a namespace included
  :wildcard? - true if wildcard query
  :id? - true if  _id was explicitly requested
  :reverse? - if this predicate is a reverse lookup. 'name' will be the non-reverse named predicate
  :as - The result key name to use in the results map for this predicate
  :recur - number of recur iterations, if specified
  :limit - limit the number of results, if specified (100 default)
  :offset - offset the number of results, if specified (0 default)
  :select - sub-selection for this predicate - only applicable to 'ref' predicates
  :compact - If we are to remove namespaces (or names from reverse refs)
  "
  [select opts]
  (let [select           (if (coll? select) (to-select-map select) select)
        default-compact? (:compact opts)]
    (reduce-kv
      (fn [acc k v]
        (let [pred     (cond (keyword? k)
                             (util/keyword->str k)

                             (symbol? k)
                             (str k)

                             :else
                             k)
              v'       (if (sequential? v) (to-select-map v) v)
              compact? (if (contains? v' "_compact")
                         (get v' "_compact")
                         default-compact?)]
          (cond
            (= "*" pred) (assoc acc :wildcard? true
                                    :compact? compact?)
            (= "_id" pred) (assoc acc :id? true)
            :else
            (let [_          (when (and v' (not (map? v)))
                               (throw (ex-info (str "Invalid select spec: " select)
                                               {:status 400
                                                :db     :db/invalid-query})))

                  sub-select (some-> (dissoc v' "_limit" "_offset" "_as" "_recur" "_component" "_orderBy" "_compact")
                                     (not-empty)
                                     (parse opts))
                  namespace? (str/includes? pred "/")
                  reverse?   (str/includes? pred "/_")
                  pred'      (if reverse? (str/replace pred "/_" "/") pred)
                  as         (cond
                               (contains? v' "_as")
                               (get v' "_as")

                               (and default-compact? reverse?)
                               (re-find #"^[^/]+" pred)

                               (and default-compact? namespace?)
                               (second (re-find #"/(.+)" pred)) ;; just capture everything after the '/'

                               :else
                               k)
                  spec       {:name             pred'
                              :wildcard?        (:wildcard? sub-select)
                              :id?              (:id? sub-select)
                              :namespace?       namespace?
                              :reverse?         reverse?
                              :componentFollow? (get v' "_component")
                              :compact?         compact?    ;; remove namespace from result if same as _collection
                              :limit            (get v' "_limit" 100)
                              :offset           (get v' "_offset" 0)
                              :as               as
                              :recur            (get v' "_recur") ;; holds max depth of recursion
                              :recur-depth      0           ;; updated with recursion depth while processing
                              :recur-seen       #{}
                              :orderBy          (get v' "_orderBy")
                              :select           (:select sub-select)}]
              (cond
                reverse?
                (assoc-in acc [:select :reverse pred'] spec)

                namespace?
                (assoc-in acc [:select :pred-id pred'] spec)

                :else
                (assoc-in acc [:select :ns-lookup pred'] spec))))))
      {} select)))


(defn p->pred-config
  [db p compact?]
  (let [name (dbproto/-p-prop db :name p)]
    {:p          p
     :limit      nil
     :name       name
     :as         (if (and compact? name)
                   (second (re-find #"/(.+)" name))
                   (or name (str p)))
     :multi?     (dbproto/-p-prop db :multi p)
     :component? (dbproto/-p-prop db :component p)
     :tag?       (= :tag (dbproto/-p-prop db :type p))
     :ref?       (dbproto/-p-prop db :ref? p)}))


(defn- build-predicate-map
  "For a flake selection, build out parts of the
  base set of predicates so we don't need to look them up
  each time... like multi, component, etc."
  [db pred-name]
  (when-let [p (dbproto/-p-prop db :id pred-name)]
    (p->pred-config db p false)))


(defn ns-lookup-pred-spec
  "Given an predicate spec produced by the parsed select statement,
  when an predicate does not have a namespace we will default it to
  utilize the namespace of the subject.

  This fills out the predicate spec that couldn't be done earlier because
  we did not know the collection."
  [db collection-id ns-lookup-spec-map]
  (let [collection-name (dbproto/-c-prop db :name collection-id)]
    (reduce-kv
      (fn [acc k v]
        (let [pred (str collection-name "/" k)]
          (if-let [p-map (build-predicate-map db pred)]
            (assoc acc (:p p-map) (merge p-map v))
            acc)))
      nil ns-lookup-spec-map)))


(defn- parse-db*
  [db sub-select]
  (loop [[[k v] & r] sub-select
         acc {}]
    (if-not k
      acc
      ;; if pred-id or reverse, substitute predicate name keys with predicate id keys
      (let [v'   (if-not (#{:pred-id :reverse} k)
                   v
                   (loop [[[k* v*] & r*] v
                          acc* {}]
                     (if-not k*
                       acc*
                       (let [p-map (build-predicate-map db k*)
                             acc** (if p-map
                                     (assoc acc* (:p p-map) (merge p-map v*))
                                     acc*)]
                         (recur r* acc**)))))
            v''  (loop [[[k* v*] & r*] v'
                        acc* {}]
                   (if-not k*
                     acc*
                     (let [acc** (if (:select v*)
                                   (assoc acc* k* (assoc v* :select (parse-db* db (:select v*))))
                                   (assoc acc* k* v*))]
                       (recur r* acc**))))
            acc' (assoc acc k v'')]
        (recur r acc')))))


;; TODO - look at caching below for speed... reset at any schema versioning
;; select statement caching, keeps cache for current schema version, then resets when schema is updated.
(def select-cache (atom {:version 0}))

(defn reset-select-cache!
  []
  (reset! select-cache {:version 0}))

(defn parse-db
  "Parses, but leverages a specific db to convert predicate
   names that it can into more complete select statement maps.

   Caches results based on database version."
  [db select opts]
  (let [schema-version (schema/version db)]
    ;; when schema is at a newer version, reset cache (version is 't' and negative, so decreases with newer)
    (when (< schema-version (:version @select-cache))
      (reset! select-cache {:version schema-version}))
    (or (get @select-cache [schema-version select opts])
        (let [select-smt  (parse select opts)
              select-smt* (assoc select-smt :select (parse-db* db (:select select-smt)))]
          (swap! select-cache assoc [schema-version select opts] select-smt*)
          select-smt*))))


(comment


  (-> @select-cache)
  (reset-select-cache!))

