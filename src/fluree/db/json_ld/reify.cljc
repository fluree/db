(ns fluree.db.json-ld.reify
  (:require [fluree.json-ld :as json-ld]
            [fluree.db.flake :as flake]
            [fluree.db.constants :as const]
            [fluree.db.json-ld.ledger :as jld-ledger]
            [fluree.db.json-ld.vocab :as vocab]
            [fluree.db.util.async :refer [<? go-try]]
            [fluree.db.util.core :as util #?(:clj :refer :cljs :refer-macros) [try* catch*]]
            [fluree.db.conn.proto :as conn-proto]
            [fluree.db.storage.core :as storage]
            [fluree.db.dbproto :as dbproto]
            [fluree.db.json-ld.commit-data :as commit-data]
            [fluree.db.index :as index]
            [fluree.db.datatype :as datatype]
            [fluree.db.util.log :as log :include-macros true]))

;; generates a db/ledger from persisted data
#?(:clj (set! *warn-on-reflection* true))

(def ^:const max-vocab-sid (flake/max-subject-id const/$_collection))

(defn node?
  "Returns true if a nested value is itself another node in the graph.
  Only need to test maps that have :id - and if they have other properties they
  are defining then we know it is a node and have additional data to include."
  [mapx]
  (cond
    (contains? mapx :value)
    false

    (and
      (contains? mapx :list)
      (= #{:list :idx} (set (keys mapx))))
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
  [{:keys [db iris sid t]} type]
  (go-try
    (if type
      (loop [[type-item & r] type
             acc []]
        (if type-item
          (let [type-id (or (<? (get-iri-sid type-item db iris))
                            (throw (ex-info
                                     (str "Retractions specifies an @type that does not exist: "
                                          type-item)
                                     {:status 400 :error :db/invalid-commit})))]
            (recur r (conj acc (flake/create sid const/$rdf:type type-id
                                             const/$xsd:anyURI t false nil))))
          acc))
      [])))

(defn- retract-v-maps
  [{:keys [sid pid t acc]} v-maps]
  (go-try
    (loop [[v-map & r-v-maps] v-maps
           acc* acc]
      (log/debug "retract v-map:" v-map)
      (let [[value dt] (datatype/from-expanded v-map nil)
            acc** (conj acc* (flake/create sid pid value dt t false nil))]
        (if (seq r-v-maps)
          (recur r-v-maps acc**)
          acc**)))))

(defn- retract-node*
  [{:keys [db type-retractions iris] :as context} node]
  (go-try
    (loop [[[k v-maps] & r] node
           acc type-retractions]
      (if k
        (if (keyword? k)
          (recur r acc)
          (let [pid (or (<? (get-iri-sid k db iris))
                        (throw (ex-info (str "Retraction on a property that does not exist: "
                                             k)
                                        {:status 400
                                         :error :db/invalid-commit})))
                _       (log/debug "retract-node* v-maps:" v-maps)
                v-maps* (if (sequential? v-maps) v-maps [v-maps])
                acc*    (<? (retract-v-maps (assoc context :pid pid :acc acc)
                                            v-maps*))]
            (recur r acc*)))
        acc))))

(defn retract-node
  [db node t iris]
  (go-try
    (let [{:keys [id type]} node
          sid              (or (<? (get-iri-sid id db iris))
                               (throw (ex-info (str "Retractions specifies an IRI that does not exist: " id
                                                    " at db t value: " t ".")
                                               {:status 400 :error
                                                :db/invalid-commit})))
          context          {:db db, :iris iris, :sid sid, :t t}
          type-retractions (if (seq type)
                             (<? (get-type-retractions context type))
                             [])
          context*         (assoc context :type-retractions type-retractions)]
      (<? (retract-node* context* node)))))

(defn retract-flakes
  [db retractions t iris]
  (go-try
    (loop [[node & r] retractions
           acc []]
      (if node
        (let [flakes (<? (retract-node db node t iris))]
          (recur r (into acc flakes)))
        acc))))

(defn- assert-v-maps
  [{:keys [db iris pid existing-pid next-pid sid next-sid id k t acc]} v-maps]
  (go-try
    (loop [[v-map & r-v-maps] v-maps
           acc* acc]
      (log/debug "assert-v-maps v-map:" v-map)
      (log/debug "assert-v-maps id:" id)
      (let [acc**
            (cond->
              (if (and id (node? v-map)) ;; is a ref to another IRI
                (let [existing-sid (<? (get-iri-sid id db iris))
                      ref-sid      (or existing-sid
                                       (jld-ledger/generate-new-sid
                                         v-map pid iris next-pid next-sid))
                      new-flake    (flake/create sid pid ref-sid
                                                 const/$xsd:anyURI t true nil)]
                  (log/debug "creating ref flake:" new-flake)
                  (cond-> (conj acc* new-flake)
                          (nil? existing-sid) (conj
                                                (flake/create ref-sid const/$iri
                                                              id
                                                              const/$xsd:string
                                                              t true nil))))
                (let [[value dt] (datatype/from-expanded v-map nil)
                      new-flake  (flake/create sid pid value dt t true nil)]
                  (log/debug "creating value flake:" new-flake)
                  (conj acc* new-flake)))
              (nil? existing-pid) (conj (flake/create pid const/$iri k
                                                      const/$xsd:string t true
                                                      nil)))]
        (if (seq r-v-maps)
          (recur r-v-maps acc**)
          acc**)))))

(defn- assert-node*
  [{:keys [base-flakes db iris next-pid refs] :as context} node]
  (go-try
    (loop [[[k v-maps] & r] node
           acc base-flakes]
      (if k
        (if (keyword? k)
          (recur r acc)
          (let [existing-pid (<? (get-iri-sid k db iris))
                v-maps*      (if (sequential? v-maps) v-maps [v-maps])
                pid          (or existing-pid
                                 (get jld-ledger/predefined-properties k)
                                 (jld-ledger/generate-new-pid
                                   k iris next-pid (-> v-maps* first :id) refs))
                acc*         (<? (assert-v-maps
                                   (assoc context :existing-pid existing-pid
                                                  :pid pid, :k k, :acc acc)
                                   v-maps*))]
            (recur r acc*)))
        acc))))

(defn- get-type-assertions
  [{:keys [db iris next-pid sid t]} type]
  (go-try
    (if type
      (loop [[type-item & r] type
             acc []]
        (if type-item
          (let [existing-id (or (<? (get-iri-sid type-item db iris))
                                (get jld-ledger/predefined-properties type-item))
                type-id     (or existing-id
                                (jld-ledger/generate-new-pid type-item iris
                                                             next-pid nil nil))
                type-flakes (when-not existing-id
                              [(flake/create type-id const/$iri type-item
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
  [db node t iris refs next-pid next-sid]
  (go-try
    (log/debug "assert-node:" node)
    (let [{:keys [id type]} node
          existing-sid    (<? (get-iri-sid id db iris))
          sid             (or existing-sid
                              (jld-ledger/generate-new-sid node nil iris
                                                           next-pid next-sid))
          context         {:db       db, :iris iris, :id id
                           :next-pid next-pid, :refs refs, :sid sid
                           :next-sid next-sid, :t t}
          type-assertions (if (seq type)
                            (<? (get-type-assertions context type))
                            [])
          base-flakes     (if existing-sid
                            type-assertions
                            (conj type-assertions
                                  (flake/create sid const/$iri id
                                                const/$xsd:string t true nil)))
          context*        (assoc context :base-flakes base-flakes)]
      (<? (assert-node* context* node)))))

(defn assert-flakes
  [db assertions t iris refs]
  (go-try
    (let [last-pid (volatile! (jld-ledger/last-pid db))
          last-sid (volatile! (jld-ledger/last-sid db))
          next-pid (fn [] (vswap! last-pid inc))
          next-sid (fn [] (vswap! last-sid inc))
          flakes   (loop [[node & r] assertions
                          acc []]
                     (if node
                       (let [assert-flakes (<? (assert-node db node t iris refs
                                                            next-pid next-sid))]
                         (recur r (into acc assert-flakes)))
                       acc))]
      {:flakes flakes
       :pid    @last-pid
       :sid    @last-sid})))


(defn merge-flakes
  [{:keys [novelty stats ecount] :as db} t refs flakes]
  (let [bytes #?(:clj (future (flake/size-bytes flakes)) ;; calculate in separate thread for CLJ
                 :cljs (flake/size-bytes flakes))
        {:keys [spot psot post opst tspo size]} novelty
        vocab-flakes  (get-vocab-flakes flakes)
        schema        (vocab/update-with db t refs vocab-flakes)
        db*           (assoc db :t t
                                :novelty {:spot (into spot flakes)
                                          :psot (into psot flakes)
                                          :post (into post flakes)
                                          :opst (->> flakes
                                                     (sort-by flake/p)
                                                     (partition-by flake/p)
                                                     (reduce
                                                       (fn [opst* p-flakes]
                                                         (let [pid (flake/p (first p-flakes))]
                                                           (if (or (refs pid) ;; refs is a set of ref pids processed in this commit
                                                                   (get-in schema [:pred pid :ref?]))
                                                             (into opst* p-flakes)
                                                             opst*)))
                                                       opst))
                                          :tspo (into tspo flakes)
                                          :size (+ size #?(:clj @bytes :cljs bytes))}
                                :stats (-> stats
                                           (update :size + #?(:clj @bytes :cljs bytes)) ;; total db ~size
                                           (update :flakes + (count flakes)))
                                :schema schema)]
    db*))

(defn commit-error
  [message commit-data]
  (throw (ex-info message {:status 400, :error :db/invalid-commit, :commit commit-data})))

(defn db-t
  "Returns 't' value from commit data."
  [db-data]
  (let [db-t (get-in db-data [const/iri-t :value])]
    (when-not (pos-int? db-t)
      (commit-error (str "Invalid, or non existent 't' value inside commit: " db-t) db-data))
    db-t))

(defn db-assert
  [db-data]
  (let [commit-assert (get-in db-data [const/iri-assert])]
    ;; TODO - any basic validation required
    commit-assert))

(defn db-retract
  [db-data]
  (let [commit-retract (get-in db-data [const/iri-retract])]
    ;; TODO - any basic validation required
    commit-retract))

(defn parse-commit
  "Given a full commit json, returns two-tuple of [commit-data commit-proof]"
  [commit-data]
  (let [cred-subj (get commit-data "https://www.w3.org/2018/credentials#credentialSubject")
        commit    (or cred-subj commit-data)]
    [commit (when cred-subj commit-data)]))

(defn read-commit
  [conn commit-address]
  (go-try
    (let [file-data (<? (conn-proto/-c-read conn commit-address))]
      (json-ld/expand file-data))))

(defn merge-commit
  [conn {:keys [ecount t] :as db} commit merged-db?]
  (go-try
    (let [iris           (volatile! {})
          refs           (volatile! (-> db :schema :refs))
          db-address     (get-in commit [const/iri-data const/iri-address :value])
          db-data        (<? (read-commit conn db-address))
          _              (log/debug "merge-commit read commit:" db-data)
          t-new          (- (db-t db-data))
          _              (when (and (not= t-new (dec t))
                                    (not merged-db?)) ;; when including multiple dbs, t values will get reused.
                           (throw (ex-info (str "Commit t value: " (- t-new)
                                                " has a gap from latest commit t value: " (- t) ".")
                                           {:status 500 :error :db/invalid-commit})))
          assert         (db-assert db-data)
          retract        (db-retract db-data)
          retract-flakes (retract-flakes db retract t-new iris)
          {:keys [flakes pid sid]} (<? (assert-flakes db assert t-new iris refs))
          all-flakes     (-> (empty (get-in db [:novelty :spot]))
                             (into (<? retract-flakes))
                             (into flakes))
          ecount*        (assoc ecount const/$_predicate pid
                                       const/$_default sid)]
      (when (empty? all-flakes)
        (commit-error "Commit has neither assertions or retractions!" commit))
      (merge-flakes (assoc db :ecount ecount*) t-new @refs all-flakes))))

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
  First commit will be t value of '1' and increment from there."
  [conn latest-commit]
  (go-try
    (loop [commit  latest-commit
           last-t  nil
           commits (list)]
      (let [dbid             (get-in commit [const/iri-data :id])
            db-address       (get-in commit [const/iri-data const/iri-address :value])
            prev-commit-addr (get-in commit [const/iri-previous const/iri-address :value])
            commit-t         (get-in commit [const/iri-data const/iri-t :value])
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
        (if (= 1 commit-t)
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
          commits (<? (trace-commits conn latest-commit))]
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
          idx-meta   (get latest-commit const/iri-index)
          db-base    (if-let [idx-address (get-in idx-meta [const/iri-address :value])]
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
        (loop [[commit & r] (<? (trace-commits conn latest-commit)) ;; TODO - can load in parallel
               db* db-base*]
          (if commit
            (let [new-db (<? (merge-commit conn db* commit merged-db?))]
              (recur r new-db))
            db*))))))

(defn load-default-context
  "Loads the default context from the given conn's storage using the given key.
  Returns a core.async channel with the context map."
  [conn key]
  (go-try
    (log/debug "loading default context from storage w/ key:" key)
    (->> key
         (conn-proto/-ctx-read conn)
         <?)))
