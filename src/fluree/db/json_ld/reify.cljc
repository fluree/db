(ns fluree.db.json-ld.reify
  (:require [fluree.json-ld :as json-ld]
            [fluree.db.flake :as flake]
            [fluree.db.constants :as const]
            [fluree.db.json-ld.ledger :as jld-ledger]
            [fluree.db.json-ld.vocab :as vocab]
            [fluree.db.util.core :as util :refer [get-first get-first-id get-first-value]]
            [fluree.db.util.async :refer [<? go-try]]
            [fluree.db.conn.proto :as conn-proto]
            [fluree.db.storage :as storage]
            [fluree.db.dbproto :as dbproto]
            [fluree.db.json-ld.commit-data :as commit-data]
            [fluree.db.index :as index]
            [fluree.db.datatype :as datatype]
            [fluree.db.util.log :as log :include-macros true]))

;; generates a db/ledger from persisted data
#?(:clj (set! *warn-on-reflection* true))

(def ^:const max-vocab-sid (flake/max-subject-id const/$_collection))

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

(defn get-iri-sid
  "Gets the IRI for any existing subject ID."
  [iri db iris]
  (go-try
    (if-let [cached (get @iris iri)]
      cached
      ;; TODO following, if a retract was made there could be 2 matching flakes and want to make sure we take the latest .-op = true
      (when-let [sid (<? (dbproto/-subid db iri))]
        (vswap! iris assoc iri sid)
        sid))))


(defn get-vocab-flakes
  [flakes]
  (flake/subrange flakes
                  >= (flake/parts->Flake [(flake/max-subject-id const/$_collection) -1])
                  <= (flake/parts->Flake [0 -1])))

(defn- get-type-retractions
  [{:keys [db iri-cache sid t]} type]
  (go-try
    (if type
      (loop [[type-item & r] type
             acc []]
        (if type-item
          (let [type-id (or (<? (get-iri-sid type-item db iri-cache))
                            (throw (ex-info
                                     (str "Retractions specifies an @type that does not exist: "
                                          type-item)
                                     {:status 400 :error :db/invalid-commit})))]
            (recur r (conj acc (flake/create sid const/$rdf:type type-id
                                             const/$xsd:anyURI t false nil))))
          acc))
      [])))

(defn- retract-v-maps
  [{:keys [db sid pid t acc iri-cache]} v-maps]
  (go-try
    (loop [[v-map & r-v-maps] v-maps
           acc* acc]
      (log/debug "retract v-map:" v-map)

      (let [ref-id (:id v-map)]
        (cond (and ref-id (node? v-map))
              (let [ref-sid (<? (get-iri-sid ref-id db iri-cache))
                    acc** (conj acc* (flake/create sid pid ref-sid const/$xsd:anyURI t false nil))]
                (if (seq r-v-maps)
                  (recur r-v-maps acc**)
                  acc**))

              :else
              (let [[value dt] (datatype/from-expanded v-map nil)
                    acc** (conj acc* (flake/create sid pid value dt t false nil))]
                (if (seq r-v-maps)
                  (recur r-v-maps acc**)
                  acc**)))))))

(defn- retract-node*
  [{:keys [db type-retractions iri-cache] :as retract-state} node]
  (go-try
    (loop [[[k v-maps] & r] node
           acc type-retractions]
      (if k
        (if (keyword? k)
          (recur r acc)
          (let [pid (or (<? (get-iri-sid k db iri-cache))
                        (throw (ex-info (str "Retraction on a property that does not exist: " k)
                                        {:status 400
                                         :error :db/invalid-commit})))
                _       (log/debug "retract-node* v-maps:" v-maps)
                v-maps* (if (sequential? v-maps) v-maps [v-maps])
                acc*    (<? (retract-v-maps (assoc retract-state :pid pid :acc acc)
                                            v-maps*))]
            (recur r acc*)))
        acc))))

(defn retract-node
  [db node t iri-cache]
  (go-try
    (let [{:keys [id type]} node
          sid              (or (<? (get-iri-sid id db iri-cache))
                               (throw (ex-info (str "Retractions specifies an IRI that does not exist: " id
                                                    " at db t value: " t ".")
                                               {:status 400 :error
                                                :db/invalid-commit})))
          retract-state        {:db db, :iri-cache iri-cache, :sid sid, :t t}
          type-retractions (if (seq type)
                             (<? (get-type-retractions retract-state type))
                             [])
          retract-state*   (assoc retract-state :type-retractions type-retractions)]
      (<? (retract-node* retract-state* node)))))

(defn retract-flakes
  [db retractions t iri-cache]
  (go-try
    (loop [[node & r] retractions
           acc []]
      (if node
        (let [flakes (<? (retract-node db node t iri-cache))]
          (recur r (into acc flakes)))
        acc))))

(defn- assert-v-maps
  [{:keys [db iri-cache pid existing-pid next-pid sid next-sid id k t acc list-members?] :as assert-state} v-maps]
  (go-try
    (loop [[v-map & r-v-maps] v-maps
           acc* acc]
      (log/debug "assert-v-maps v-map:" v-map)
      (log/debug "assert-v-maps id:" id)
      (let [ref-id (:id v-map)
            meta (when list-members? {:i (-> v-map :idx last)})
            acc**
            (cond->
                (cond
                  (and ref-id (node? v-map))
                  (let [existing-sid (<? (get-iri-sid ref-id db iri-cache))
                        ref-sid      (or existing-sid
                                         (jld-ledger/generate-new-sid
                                           v-map pid iri-cache next-pid next-sid))
                        new-flake    (flake/create sid pid ref-sid
                                                   const/$xsd:anyURI t true meta)]
                    (log/debug "creating ref flake:" new-flake)
                    (cond-> (conj acc* new-flake)
                      (nil? existing-sid) (conj
                                            (flake/create ref-sid const/$xsd:anyURI
                                                          ref-id
                                                          const/$xsd:string
                                                          t true nil))))
                  (list-value? v-map)
                  (let [list-vals (:list v-map)]
                    (<? (assert-v-maps (assoc assert-state :list-members? true) list-vals)))

                  :else (let [[value dt] (datatype/from-expanded v-map nil)
                              new-flake  (flake/create sid pid value dt t true meta)]
                          (log/debug "creating value flake:" new-flake)
                          (conj acc* new-flake)))

              (nil? existing-pid) (conj (flake/create pid const/$xsd:anyURI k
                                                      const/$xsd:string t true
                                                      nil)))]
        (if (seq r-v-maps)
          (recur r-v-maps acc**)
          acc**)))))

(defn- assert-node*
  [{:keys [base-flakes db iri-cache next-pid ref-cache] :as assert-state} node]
  (go-try
    (loop [[[k v-maps] & r] node
           acc base-flakes]
      (if k
        (if (keyword? k)
          (recur r acc)
          (let [existing-pid (<? (get-iri-sid k db iri-cache))
                v-maps*      (util/sequential v-maps)
                pid          (or existing-pid
                                 (get jld-ledger/predefined-properties k)
                                 (jld-ledger/generate-new-pid
                                   k iri-cache next-pid (-> v-maps* first :id) ref-cache))
                acc*         (<? (assert-v-maps
                                   (assoc assert-state :existing-pid existing-pid
                                                  :pid pid, :k k, :acc acc)
                                   v-maps*))]
            (recur r acc*)))
        acc))))

(defn- get-type-assertions
  [{:keys [db iri-cache next-pid sid t]} type]
  (go-try
    (if type
      (loop [[type-item & r] type
             acc []]
        (if type-item
          (let [existing-id (<? (get-iri-sid type-item db iri-cache))
                type-id     (or existing-id
                                (get jld-ledger/predefined-properties type-item)
                                (jld-ledger/generate-new-pid type-item iri-cache
                                                             next-pid nil nil))
                type-flakes (when-not existing-id
                              [(flake/create type-id const/$xsd:anyURI type-item
                                             const/$xsd:string t true nil)
                               (flake/create type-id const/$rdf:type
                                             const/$rdfs:Class const/$xsd:anyURI
                                             t true nil)])]
            (recur r (cond-> (conj acc
                                   (flake/create sid const/$rdf:type type-id
                                                 const/$xsd:anyURI t true nil))
                             type-flakes (into type-flakes))))
          acc))
      [])))

(defn assert-node
  [db node t iri-cache ref-cache next-pid next-sid]
  (go-try
    (log/debug "assert-node:" node)
    (let [{:keys [id type]} node
          existing-sid    (<? (get-iri-sid id db iri-cache))
          sid             (or existing-sid
                              (jld-ledger/generate-new-sid node nil iri-cache
                                                           next-pid next-sid))
          assert-state    {:db db, :iri-cache iri-cache, :id id
                           :next-pid next-pid, :ref-cache ref-cache, :sid sid
                           :next-sid next-sid, :t t}
          type-assertions (if (seq type)
                            (<? (get-type-assertions assert-state type))
                            [])
          base-flakes     (if existing-sid
                            type-assertions
                            (conj type-assertions
                                  (flake/create sid const/$xsd:anyURI id
                                                const/$xsd:string t true nil)))
          assert-state*   (assoc assert-state :base-flakes base-flakes)]
      (<? (assert-node* assert-state* node)))))

(defn assert-flakes
  [db assertions t iri-cache ref-cache]
  (go-try
    (let [last-pid (volatile! (jld-ledger/last-pid db))
          last-sid (volatile! (jld-ledger/last-sid db))
          next-pid (fn [] (vswap! last-pid inc))
          next-sid (fn [] (vswap! last-sid inc))
          flakes   (loop [[node & r] assertions
                          acc []]
                     (if node
                       (let [assert-flakes (<? (assert-node db node t iri-cache ref-cache
                                                            next-pid next-sid))]
                         (recur r (into acc assert-flakes)))
                       acc))]
      {:flakes flakes
       :pid    @last-pid
       :sid    @last-sid})))

(defn merge-flakes
  [db t refs flakes]
  (let [vocab-flakes  (get-vocab-flakes flakes)]
    (-> db
        (assoc :t t)
        (commit-data/update-novelty flakes)
        (update :schema vocab/update-with t refs vocab-flakes))))

(defn commit-error
  [message commit-data]
  (throw
   (ex-info message
            {:status 400, :error :db/invalid-commit, :commit commit-data})))

(defn db-t
  "Returns 't' value from commit data."
  [db-data]
  (let [db-t (get-first-value db-data const/iri-t)]
    (when-not (pos-int? db-t)
      (commit-error
       (str "Invalid, or non existent 't' value inside commit: " db-t) db-data))
    db-t))

(defn enrich-values
  [id->node values]
  (mapv (fn [{:keys [id list type] :as v-map}]
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

(defn parse-commit
  "Given a full commit json, returns two-tuple of [commit-data commit-proof]"
  [commit-data]
  (let [cred-subj (get-first commit-data const/iri-cred-subj)
        commit    (or cred-subj commit-data)]
    [commit (when cred-subj commit-data)]))

(defn read-commit
  [conn commit-address]
  (go-try
    (let [file-data (<? (conn-proto/-c-read conn commit-address))
          addr-key-path (if (contains? file-data "credentialSubject")
                          ["credentialSubject" "address"]
                          ["address"])
          commit    (assoc-in file-data addr-key-path commit-address)]
      (log/trace "read-commit commit:" commit)
      (json-ld/expand commit))))

(defn read-db
  [conn db-address]
  (go-try
    (let [file-data (<? (conn-proto/-c-read conn db-address))
          db        (assoc file-data "f:address" db-address)]
      (json-ld/expand db))))

(defn merge-commit
  [conn {:keys [ecount t] :as db} commit merged-db?]
  (go-try
   (let [iri-cache          (volatile! {})
         refs-cache         (volatile! (-> db :schema :refs))
         db-address         (-> commit
                                (get-first const/iri-data)
                                (get-first-value const/iri-address))
         db-data            (<? (read-db conn db-address))
         t-new              (- (db-t db-data))
         _                  (when (and (not= t-new (dec t))
                                       (not merged-db?)) ;; when including multiple dbs, t values will get reused.
                              (throw (ex-info (str "Commit t value: " (- t-new)
                                                   " has a gap from latest commit t value: " (- t) ".")
                                              {:status 500 :error :db/invalid-commit})))
         assert             (db-assert db-data)
         retract            (db-retract db-data)
         retract-flakes     (<? (retract-flakes db retract t-new iri-cache))
         {:keys [flakes pid sid]} (<? (assert-flakes db assert t-new iri-cache refs-cache))

         {:keys [previous issuer message defaultContext] :as commit-metadata}
         (commit-data/json-ld->map commit db)

         [prev-commit _] (some->> previous :address (read-commit conn) <?
                                  parse-commit)
         last-sid           (volatile! (jld-ledger/last-commit-sid db))
         next-sid           (fn [] (vswap! last-sid inc))
         db-sid             (next-sid)
         metadata-flakes    (commit-data/commit-metadata-flakes commit-metadata
                                                                t-new db-sid)
         previous-id        (when prev-commit (:id prev-commit))
         prev-commit-flakes (when previous-id
                              (<? (commit-data/prev-commit-flakes db t-new
                                                                  previous-id)))
         prev-data-id       (get-first-id prev-commit const/iri-data)
         prev-db-flakes     (when prev-data-id
                              (<? (commit-data/prev-data-flakes db db-sid t-new
                                                                prev-data-id)))
         issuer-flakes      (when-let [issuer-iri (:id issuer)]
                              (<? (commit-data/issuer-flakes db t-new next-sid
                                                             issuer-iri)))
         message-flakes     (when message
                              (commit-data/message-flakes t-new message))
         default-ctx-flakes (when defaultContext
                              (<? (commit-data/default-ctx-flakes
                                   db t-new next-sid defaultContext)))
         all-flakes         (-> db
                                (get-in [:novelty :spot])
                                empty
                                (into metadata-flakes)
                                (into retract-flakes)
                                (into flakes)
                                (cond->
                                 prev-commit-flakes
                                 (into prev-commit-flakes)
                                 prev-db-flakes
                                 (into prev-db-flakes)
                                 issuer-flakes
                                 (into issuer-flakes)
                                 message-flakes
                                 (into message-flakes)
                                 default-ctx-flakes
                                 (into default-ctx-flakes)
                                 (= -1 t-new)
                                 (into commit-data/commit-schema-flakes)))
         ecount*            (assoc ecount const/$_predicate pid
                                          const/$_default sid
                                          const/$_shard @last-sid)]
     (when (empty? all-flakes)
       (commit-error "Commit has neither assertions or retractions!"
                     commit-metadata))
     (merge-flakes (assoc db :ecount ecount*) t-new @refs-cache all-flakes))))

;; TODO - validate commit signatures
(defn validate-commit
  "Run proof validation, if exists.
  Return actual commit data. In the case of a VerifiableCredential this is
  the `credentialSubject`."
  [db commit proof]
  ;; TODO - returning true for now
  true)

(defn trace-commits
  "Returns a list of two-tuples each containing [commit proof] as applicable.
  First commit will be t value of `from-t` and increment from there."
  [conn latest-commit from-t]
  (go-try
    (loop [commit  latest-commit
           last-t  nil
           commits (list)]
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
            commits*         (conj commits commit)]
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
          commits*
          (let [commit-data (<? (read-commit conn prev-commit-addr))
                [commit proof] (parse-commit commit-data)]
            (when proof ;; TODO
              (validate-commit nil commit proof))
            (recur commit commit-t commits*)))))))


(defn load-db
  [{:keys [ledger] :as db} latest-commit merged-db?]
  (go-try
    (let [{:keys [conn]} ledger
          commits (<? (trace-commits conn latest-commit 1))]
      (loop [[commit & r] commits
             db* db]
        (if commit
          (let [new-db (<? (merge-commit conn db* commit merged-db?))]
            (recur r new-db))
          db*)))))


(defn load-db-idx
  [{:keys [ledger] :as db} latest-commit commit-address merged-db?]
  (go-try
    (let [{:keys [conn]} ledger
          idx-meta   (get-first latest-commit const/iri-index) ; get persistent
                                                               ; index meta if
                                                               ; ledger has
                                                               ; indexes
          db-base    (if-let [idx-address (get-first-value idx-meta const/iri-address)]
                       (<? (storage/reify-db conn db idx-address))
                       db)
          commit-map (commit-data/json-ld->map latest-commit
                                               (-> (select-keys db-base index/types)
                                                   (assoc :commit-address commit-address)))
          _          (log/debug "load-db-idx commit-map:" commit-map)
          db-base*   (assoc db-base :commit commit-map)
          index-t    (commit-data/index-t commit-map)
          commit-t   (commit-data/t commit-map)]
      (if (= commit-t index-t)
        db-base* ;; if index-t is same as latest commit, no additional commits to load
        ;; trace to the first unindexed commit TODO - load in parallel
        (loop [[commit & r] (<? (trace-commits conn latest-commit (if index-t
                                                                    (inc index-t)
                                                                    1)))
               db* db-base*]
          (if commit
            (let [new-db (<? (merge-commit conn db* commit merged-db?))]
              (recur r new-db))
            db*))))))

(defn load-default-context
  "Loads the default context from the given conn's storage using the given address.
  Returns a core.async channel with the context map."
  [conn address]
  (go-try
   (log/debug "loading default context from storage w/ address:" address)
   (->> address
        (conn-proto/-ctx-read conn)
        <?)))
