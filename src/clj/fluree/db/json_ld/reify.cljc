(ns fluree.db.json-ld.reify
  (:require [fluree.json-ld :as json-ld]
            [fluree.db.flake :as flake]
            [clojure.set :refer [map-invert]]
            [fluree.db.constants :as const]
            [fluree.db.json-ld.vocab :as vocab]
            [fluree.db.util.core :as util :refer [get-first get-first-id get-first-value]]
            [fluree.db.util.async :refer [<? go-try]]
            [fluree.db.connection :as connection]
            [fluree.db.json-ld.commit-data :as commit-data]
            [fluree.db.index :as index]
            [fluree.db.datatype :as datatype]
            [fluree.db.util.log :as log :include-macros true]
            [fluree.db.json-ld.iri :as iri]))

;; generates a db/ledger from persisted data
#?(:clj (set! *warn-on-reflection* true))

(defn list-value?
  "returns true if json-ld value is a list object."
  [v]
  (and (map? v)
       (= :list (-> v first key))))

(defn node?
  "Returns true if a nested value is itself another node in the graph.
  Only need to test maps that have :id - and if they have other properties they
  are defining then we know it is a node and have additional data to include."
  [mapx]
  (cond
    (contains? mapx :value)
    false

    (list-value? mapx)
    false

    (and
      (contains? mapx :set)
      (= #{:set :idx} (set (keys mapx))))
    false

    :else
    true))

(defn- get-type-retractions
  [db t sid type]
  (into []
        (map (fn [type-item]
               (let [type-sid (iri/encode-iri db type-item)]
                 (flake/create sid const/$rdf:type type-sid
                               const/$xsd:anyURI t false nil))))
        type))

(defn retract-value-map
  [db sid pid t v-map]
  (let [ref-id (:id v-map)]
    (if (and ref-id (node? v-map))
      (let [ref-sid (iri/encode-iri db ref-id)]
        (flake/create sid pid ref-sid const/$xsd:anyURI t false nil))
      (let [[value dt] (datatype/from-expanded v-map nil)]
        (flake/create sid pid value dt t false nil)))))

(defn- retract-node*
  [db t {:keys [sid type-retractions] :as retract-state} node]
  (loop [[[k v-maps] & r] node
         acc              type-retractions]
    (if k
      (if (keyword? k)
        (recur r acc)
        (let [pid  (or (iri/encode-iri db k)
                       (throw (ex-info (str "Retraction on a property that does not exist: " k)
                                       {:status 400
                                        :error  :db/invalid-commit})))
              acc* (into acc
                         (map (partial retract-value-map db sid pid t))
                         (util/sequential v-maps))]
          (recur r acc*)))
      acc)))

(defn retract-node
  [db t node]
  (let [{:keys [id type]} node
        sid               (or (iri/encode-iri db id)
                              (throw (ex-info (str "Retractions specifies an IRI that does not exist: " id
                                                   " at db t value: " t ".")
                                              {:status 400 :error
                                               :db/invalid-commit})))
        retract-state     {:sid sid}
        type-retractions  (if (seq type)
                            (get-type-retractions db t sid type)
                            [])
        retract-state*    (assoc retract-state :type-retractions type-retractions)]
    (retract-node* db t retract-state* node)))

(defn retract-flakes
  [db t retractions]
  (into []
        (mapcat (partial retract-node db t))
        retractions))

(defn- get-type-assertions
  [db t sid type]
  (if type
    (loop [[type-item & r] type
           acc             []]
      (if type-item
        (let [type-id (iri/encode-iri db type-item)]
          (recur r  (conj acc (flake/create sid const/$rdf:type type-id const/$xsd:anyURI t true nil))))
        acc))
    []))

(defn assert-value-map
  [db sid pid t v-map]
  (let [ref-id (:id v-map)
        meta   (::meta v-map)]
    (if (and ref-id (node? v-map))
      (let [ref-sid (iri/encode-iri db ref-id)]
        (flake/create sid pid ref-sid const/$xsd:anyURI t true meta))
      (let [[value dt] (datatype/from-expanded v-map nil)]
        (flake/create sid pid value dt t true meta)))))

(defn add-list-meta
  [list-val]
  (let [m {:i (-> list-val :idx last)}]
    (assoc list-val ::meta m)))

(defn assert-property
  [db sid pid t value]
  (let [v-maps (util/sequential value)]
    (mapcat (fn [v-map]
              (if (list-value? v-map)
                (let [list-vals (:list v-map)]
                  (into []
                        (comp (map add-list-meta)
                              (map (partial assert-value-map db sid pid t)))
                        list-vals))
                [(assert-value-map db sid pid t v-map)]))
            v-maps)))

(defn assert-node
  [db t node]
  (log/trace "assert-node:" node)
  (let [{:keys [id type]} node
        sid             (iri/encode-iri db id)
        type-assertions (if (seq type)
                          (get-type-assertions db t sid type)
                          [])]
    (into type-assertions
          (comp (filter (fn [node-entry]
                          (not (-> node-entry key keyword?))))
                (mapcat (fn [[prop value]]
                          (let [pid (iri/encode-iri db prop)]
                            (assert-property db sid pid t value)))))
          node)))

(defn assert-flakes
  [db t assertions]
  (into []
        (mapcat (partial assert-node db t))
        assertions))

(defn merge-flakes
  "Returns updated db with merged flakes."
  [db t flakes]
  (-> db
      (assoc :t t)
      (commit-data/update-novelty flakes)
      (vocab/hydrate-schema flakes)))

(defn commit-error
  [message commit-data]
  (throw
    (ex-info message
             {:status 400, :error :db/invalid-commit, :commit commit-data})))

(defn db-t
  "Returns 't' value from commit data."
  [db-data]
  (let [t (get-first-value db-data const/iri-t)]
    (when-not (pos-int? t)
      (commit-error
        (str "Invalid, or non existent 't' value inside commit: " t) db-data))
    t))

(defn enrich-values
  [id->node values]
  (mapv (fn [{:keys [id type] :as v-map}]
          (if id
            (merge (get id->node id)
                   (cond-> v-map
                     (nil? type) (dissoc :type)))
            v-map))
        values))

(defn enrich-node
  [id->node node]
  (reduce-kv
    (fn [updated-node k v]
      (assoc updated-node k (cond (= :id k)         v
                                  (:list (first v)) [{:list (enrich-values id->node (:list (first v)))}]
                                  :else             (enrich-values id->node v))))
    {}
    node))

(defn enrich-assertion-values
  "`asserts` is a json-ld flattened (ish) sequence of nodes. In order to properly generate
  sids (or pids) for these nodes, we need the full node additional context for ref objects. This
  function traverses the asserts and builds a map of node-id->node, then traverses the
  asserts again and merges each ref object into the ref's node.

  example input:
  [{:id \"foo:bar\"
    \"ex:key1\" {:id \"foo:ref-id\"}}
  {:id \"foo:ref-id\"
   :type \"some:type\"}]

  example output:
  [{:id \"foo:bar\"
    \"ex:key1\" {:id \"foo:ref-id\"
                 :type \"some:type\"}}
  {:id \"foo:ref-id\"
   :type \"some:type\"}]
  "
  [asserts]
  (let [id->node (reduce (fn [id->node {:keys [id] :as node}] (assoc id->node id node))
                         {}
                         asserts)]
    (mapv (partial enrich-node id->node)
          asserts)))

(defn db-assert
  [db-data]
  (let [commit-assert (get db-data const/iri-assert)]
    ;; TODO - any basic validation required
    (enrich-assertion-values commit-assert)))

(defn db-retract
  [db-data]
  (let [commit-retract (get db-data const/iri-retract)]
    ;; TODO - any basic validation required
    commit-retract))

;; TODO - validate commit signatures
(defn validate-commit-proof
  "Run proof validation, if exists.
  Return actual commit data. In the case of a VerifiableCredential this is
  the `credentialSubject`."
  [proof]
  ;; TODO - returning true for now
  true)

(defn has-proof?
  [commit-data]
  (contains? commit-data const/iri-cred-subj))

(defn verify-commit
  "Given a full commit json, returns two-tuple of [commit-data commit-proof]"
  [commit-data]
  (if (has-proof? commit-data)
    (let [credential-subject (get-first commit-data const/iri-cred-subj)]
      (validate-commit-proof commit-data)
      [credential-subject commit-data])
    [commit-data nil]))

(defn read-commit
  [conn commit-address]
  (go-try
    (let [commit-data   (<? (connection/-c-read conn commit-address))
          addr-key-path (if (contains? commit-data "credentialSubject")
                          ["credentialSubject" "address"]
                          ["address"])]
      (log/trace "read-commit at:" commit-address "data:" commit-data)
      (when commit-data
        (-> commit-data
            (assoc-in addr-key-path commit-address)
            json-ld/expand
            verify-commit)))))

(defn read-db
  [conn db-address]
  (go-try
    (let [file-data (<? (connection/-c-read conn db-address))
          db        (assoc file-data "f:address" db-address)]
      (json-ld/expand db))))

(defn with-namespaces
  [{:keys [namespaces max-namespace-code] :as db} new-namespaces]
  (let [new-ns-map          (into namespaces
                                  (map-indexed (fn [i ns]
                                                 (let [ns-code (+ (inc i)
                                                                  max-namespace-code)]
                                                   [ns ns-code])))
                                  new-namespaces)
        new-ns-codes        (map-invert new-ns-map)
        max-namespace-code* (apply max (vals new-ns-map))]
    (assoc db
           :namespaces new-ns-map
           :namespace-codes new-ns-codes
           :max-namespace-code max-namespace-code*)))

(defn merge-commit
  "Process a new commit map, converts commit into flakes, updates
  respective indexes and returns updated db"
  [conn db [commit _proof]]
  (go-try
    (let [db-address       (-> commit
                               (get-first const/iri-data)
                               (get-first-value const/iri-address))
          db-data          (<? (read-db conn db-address))
          t-new            (db-t db-data)
          assert           (db-assert db-data)
          nses             (map :value
                                (get db-data const/iri-namespaces))
          db*              (with-namespaces db nses)
          asserted-flakes  (assert-flakes db* t-new assert)
          retract          (db-retract db-data)
          retracted-flakes (retract-flakes db* t-new retract)

          {:keys [previous issuer message data] :as commit-metadata}
          (commit-data/json-ld->map commit db*)

          commit-id          (:id commit-metadata)
          commit-sid         (iri/encode-iri db* commit-id)
          [prev-commit _]    (some->> previous :address (read-commit conn) <?)
          db-sid             (iri/encode-iri db* (:id data))
          metadata-flakes    (commit-data/commit-metadata-flakes commit-metadata
                                                                 t-new commit-sid db-sid)
          previous-id        (when prev-commit (:id prev-commit))
          prev-commit-flakes (when previous-id
                               (commit-data/prev-commit-flakes db* t-new commit-sid
                                                               previous-id))
          prev-data-id       (get-first-id prev-commit const/iri-data)
          prev-db-flakes     (when prev-data-id
                               (commit-data/prev-data-flakes db* db-sid t-new
                                                             prev-data-id))
          issuer-flakes      (when-let [issuer-iri (:id issuer)]
                               (commit-data/issuer-flakes db* t-new commit-sid issuer-iri))
          message-flakes     (when message
                               (commit-data/message-flakes t-new commit-sid message))
          all-flakes         (-> db*
                                 (get-in [:novelty :spot])
                                 empty
                                 (into metadata-flakes)
                                 (into retracted-flakes)
                                 (into asserted-flakes)
                                 (cond->
                                     prev-commit-flakes (into prev-commit-flakes)
                                     prev-db-flakes (into prev-db-flakes)
                                     issuer-flakes  (into issuer-flakes)
                                     message-flakes (into message-flakes)))]
      (when (empty? all-flakes)
        (commit-error "Commit has neither assertions or retractions!"
                      commit-metadata))
      (-> db*
          (merge-flakes t-new all-flakes)
          (assoc :commit commit-metadata)))))


(defn trace-commits
  "Returns a list of two-tuples each containing [commit proof] as applicable.
  First commit will be t value of `from-t` and increment from there."
  [conn latest-commit-tuple from-t]
  (go-try
    (loop [[commit proof] latest-commit-tuple
           last-t        nil
           commit-tuples (list)]
      (let [dbid             (get-first-id commit const/iri-data)
            db-address       (-> commit
                                 (get-first const/iri-data)
                                 (get-first-value const/iri-address))
            prev-commit-addr (-> commit
                                 (get-first const/iri-previous)
                                 (get-first-value const/iri-address))
            commit-t         (-> commit
                                 (get-first const/iri-data)
                                 (get-first-value const/iri-t))
            commit-tuples*   (conj commit-tuples [commit proof])]
        (when (or (nil? commit-t)
                  (and last-t (not= (dec last-t) commit-t)))
          (throw (ex-info (str "Commit t values are inconsistent. Last commit t was: " last-t
                               "and the prevCommit t value is: " commit-t " for commit: " commit)
                          {:status      500
                           :error       :db/invalid-commit
                           :commit-data (if (> (count (str commit)) 500)
                                          (str (subs (str commit) 0 500) "...")
                                          (str commit))})))
        (when-not (and dbid db-address)
          (throw (ex-info (str "Commit is not a properly formatted Fluree commit, missing db id/address: "
                               commit ".")
                          {:status      500
                           :error       :db/invalid-commit
                           :commit-data (if (> (count (str commit)) 500)
                                          (str (subs (str commit) 0 500) "...")
                                          (str commit))})))
        (if (= from-t commit-t)
          commit-tuples*
          (let [commit-tuple (<? (read-commit conn prev-commit-addr))]
            (recur commit-tuple commit-t commit-tuples*)))))))
