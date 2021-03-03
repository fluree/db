(ns fluree.db.util.tx
  (:require [fluree.db.util.json :as json]
            [fluree.crypto :as crypto]
            [clojure.string :as str]
            [fluree.db.util.async :refer [<? go-try]]
            [fluree.db.flake :as flake]
            [fluree.db.api :as fdb]
            [fluree.db.util.core :as util]
            [fluree.db.dbproto :as dbproto]
            [fluree.db.dbfunctions.core :as dbfunctions]
            [fluree.db.util.async :as async-util]
            [fluree.db.permissions-validate :as perm-validate]
            [fluree.db.constants :as const])
  (:import (fluree.db.flake Flake)))

;; transaction utilities


(defn validate-command
  "Takes a command (map) and validates signature, adds in auth or authority and does
  some additional checks. This can be done before putting the command into the queue for processing.

  Puts original :cmd string and :sig string into this one map for use downstream."
  [{:keys [sig cmd]}]
  ;; TODO - here again we calc the sha3 id, I think redundant at this point
  (let [cmd-map       (-> (json/parse cmd)
                          (assoc :txid (crypto/sha3-256 cmd) ;; don't trust their id if provided
                                 :cmd cmd
                                 :sig sig))

        sig-authority (crypto/account-id-from-message cmd sig) ;; throws if invalid signature
        ;; merge everything together into one map for transaction.
        current-time  (System/currentTimeMillis)
        {:keys [auth authority expire]} cmd-map
        expired?      (and expire (< expire current-time))
        _             (when expired?
                        (throw (ex-info (format "Transaction is expired. Current time: %s expire time: %s." current-time expire)
                                        {:status 400 :error :db/invalid-transaction})))
        cmd-map*      (cond
                        (and (nil? auth) (nil? authority))
                        (assoc cmd-map :auth sig-authority)

                        (and (nil? auth) authority)
                        (throw (ex-info (str "An authority without an auth is not allowed.")
                                        {:status 400 :error :db/invalid-transaction}))

                        (and auth authority)
                        (if (= authority sig-authority)
                          cmd-map
                          (throw (ex-info (format "Signing authority: %s does not match command authority: %s." sig-authority authority)
                                          {:status 400 :error :db/invalid-transaction})))

                        (and auth (nil? authority))
                        (if (= auth sig-authority)
                          cmd-map
                          (assoc cmd-map :authority sig-authority)))]
    cmd-map*))


(defn get-tx-meta-from-tx
  "Separates tx-meta from the rest of the transaction.
  If by chance tx-meta was included twice, will throw an exception."
  [txn]
  (let [grouped (group-by #(if (str/starts-with? (:_id %) "_tx")
                             :tx-meta
                             :rest-tx) txn)
        tx-meta (when-let [tx-meta+ (not-empty (:tx-meta grouped))]
                  (when (not= 1 (count tx-meta+))
                    (throw (ex-info "You have multiple _tx metadata records in a single transaction, only one is allowed."
                                    {:status 400 :error :db/invalid-transaction})))
                  (->> tx-meta+
                       first
                       (reduce-kv (fn [acc k v]
                                    (cond
                                      (or (= :_id k) (= :_action k) (= :_meta k))
                                      (assoc acc k v)

                                      (nil? (namespace k))
                                      (assoc acc (keyword "_tx" (name k)) v)

                                      :else
                                      (assoc acc k v)))
                                  {})))]
    {:tx-meta tx-meta
     :rest-tx (:rest-tx grouped)}))

(defn gen-tx-hash
  "From a list of transaction flakes, returns the sha3 hash.

  Note, this assumes the _tx/hash flake is NOT included in this list,
  else the resulting hash will be different from the one that would have
  been computed when performing the transaction."
  ([tx-flakes]
   ;; sort in block sort order as defined by fluree.db.flake/cmp-flakes-block
   (-> (apply flake/sorted-set-by flake/cmp-flakes-block tx-flakes)
       (gen-tx-hash true)))
  ([tx-flakes sorted?]
   (if-not sorted?
     (gen-tx-hash tx-flakes)
     (->> tx-flakes
          (mapv #(vector (.-s %) (.-p %) (.-o %) (.-t %) (.-op %) (.-m %)))
          (json/stringify)
          (crypto/sha3-256)))))


;;;
;;; Block merkle root calculation
;;;

(defn- exp [x n]
  (loop [acc 1 n n]
    (if (zero? n) acc
                  (recur (* x acc) (dec n)))))

(defn- find-closest-power-2
  [n]
  (loop [i 1]
    (if (>= (exp 2 i) n)
      (exp 2 i)
      (recur (inc i)))))

(defn- generate-hashes
  [cmds]
  (loop [[f s & r] cmds
         acc []]
    (let [hash (crypto/sha2-256 (str f s))
          acc* (conj acc hash)]
      (if r
        (recur r acc*)
        acc*))))

(defn generate-merkle-root
  "hashes should already be in the correct order."
  [& hashes]
  (let [count-cmds   (count hashes)
        repeat-last  (- count-cmds (find-closest-power-2 count-cmds))
        leaves-ordrd (concat hashes (repeat repeat-last (last hashes)))]
    (loop [merkle-results (apply generate-hashes leaves-ordrd)]
      (if (> 1 (count merkle-results))
        (recur (apply generate-hashes merkle-results))
        (first merkle-results)))))

(defn deps-succeeded?
  "Returns true if list of dependency transactions are satisfied."
  [db deps]
  (go-try (if (or (not deps) (empty? deps))
            true
            (let [res (->> (reduce-kv (fn [query-acc key dep]
                                        (-> query-acc
                                            (update :selectOne conj (str "?error" key))
                                            (update :where conj [(str "?tx" key) "_tx/id" dep])
                                            (update :optional conj [(str "?tx" key) "_tx/error" (str "?error" key)])))
                                      {:selectOne [] :where [] :optional []} deps)
                           (fdb/query-async (go-try db))
                           <?)]
              (and (not (empty? res)) (every? nil? res))))))

;; TODO - moved this from the original transact namespace. Need to look at how this special treatment is handled
;; and verify it is being done in a reasonable way.
(defn create-new-db-tx
  [tx-map]
  (let [{:keys [db alias auth doc fork forkBlock]} tx-map
        db-name (if (sequential? db)
                  (str (first db) "/" (second db))
                  (str/replace db "/$" "/"))
        tx      (util/without-nils
                  {:_id       "db$newdb"
                   :_action   :insert
                   :id        db-name
                   :alias     (or alias db-name)
                   :root      auth
                   :doc       doc
                   :fork      fork
                   :forkBlock forkBlock})]
    [tx]))


(defn make-candidate-db
  "Assigns a tempid to all index roots, which ensures caching for this candidate db
  is independent from any 'official' db with the same block."
  [db]
  (let [tempid  (util/random-uuid)
        indexes [:spot :psot :post :opst]]
    (reduce
      (fn [db idx]
        (let [index (assoc (get db idx) :tempid tempid)]
          (assoc db idx index)))
      db indexes)))

(defn adding-data?
  "Returns true upon finding first 'true' .-op for a flake other than the transaction
  flake.

  Note this would not catch an update to metadata of a prior transaction."
  [new-flakes]
  (some #(if (< 0 (.-s %))
           (.-op %) false)
        new-flakes))

;; TODO - this should use 'some' form to fail after fist spec fails (it currently does everything, even if a failure occurs)
;; TODO - spec for collections should be cached with the db's :schema, pointing to sids of SmartFunctions - and can use a caching fn then to get respective SmartFunctions by id
(defn validate-collection-spec
  [db-after flakes auth_id block-instant]
  (go-try
    (let [all-sids       (->> (map #(.-s %) flakes)         ;; Get all unique subjects in flakes
                              (set))
          ;; Get collection-sids for all subject collections
          collection-sid (mapv #(->> (flake/sid->cid %) (dbproto/-c-prop db-after :sid)) all-sids)

          ;; Get spec and specDoc for all entities
          specs-res      (loop [[sid & r] collection-sid
                                acc []]
                           (if-not sid
                             acc
                             (let [res      (<? (dbproto/-query db-after
                                                                {:selectOne [{"_collection/spec" ["_fn/code" "_fn/params"]} "_collection/specDoc"]
                                                                 :from      sid}))
                                   specs    (get res "_collection/spec")
                                   spec     (map #(get % "_fn/code") specs)
                                   params   (remove nil? (map #(get % "_fn/params") specs))
                                   _        (if (empty? params)
                                              nil
                                              (throw (ex-info (str "You can only use functions with additional parameters in transactions functions. ")
                                                              {:status 400
                                                               :error  :db/invalid-tx})))
                                   spec-str (dbfunctions/combine-fns spec)
                                   spec-doc (get res "_collection/specDoc")]
                               (recur r (conj acc (vector spec-str spec-doc))))))
          ;; Create a vector for all [subject spec]
          spec-vec       (map (fn [e [spec spec-doc]]
                                (vector e spec spec-doc))
                              all-sids specs-res)
          ;; Remove any entities from spec-vec that don't have specs
          spec-vec*      (remove (fn [n]
                                   (nil? (second n))) spec-vec)

          ;; Search for each subject in the candidate-db. If null, then the entire subject was deleted
          entities*      (loop [[[sid spec spec-doc] & r] spec-vec*
                                acc []]
                           (if-not sid
                             acc
                             (let [res (<? (dbproto/-query db-after {:selectOne ["*"] :from sid}))]
                               (recur r (conj acc res)))))
          spec-vec**     (remove nil? (map (fn [ent spec-vec]
                                             (if ent
                                               spec-vec
                                               nil))
                                           entities* spec-vec*))
          ctx            (remove nil? (map (fn [subject]
                                             (if subject
                                               {:db      db-after
                                                :instant block-instant
                                                :sid     (get subject "_id")
                                                :flakes  flakes
                                                :auth_id auth_id
                                                :state   (atom {:stack   []
                                                                :credits 10000000
                                                                :spent   0})})) entities*))
          f-meta         (loop [[[sid spec spec-doc params] & r] spec-vec**
                                acc []]
                           (if-not sid
                             acc
                             (recur r (conj acc (<? (dbfunctions/parse-fn db-after spec "collectionSpec" nil))))))]
      (loop
        [[f & r] f-meta
         [ctx & ctx-r] ctx
         [spec & spec-r] spec-vec**]
        (if f
          (let [res  (f ctx)
                res* (if (async-util/channel? res) (<? res) res)]
            (cond (not res*)
                  (throw (ex-info (str "Transaction does not adhere to the collection spec: " (nth spec 2))
                                  {:status 400
                                   :error  :db/invalid-tx}))

                  r
                  (recur r ctx-r spec-r)

                  :else true)) true)))))

;; TODO - if no predicate spec exists, could skip all of this - look to add to db's :schema
(defn valid-predicate-spec-flake?
  "Takes a db and a flake, checks whether the flake adheres to any _predicate/spec"
  [flake db auth_id]
  (go-try
    (let [pid      (.-p flake)
          pred-map (->> db :schema :pred ((fn [coll]
                                            (get coll pid))))
          spec     (:spec pred-map)
          specDoc  (:specDoc pred-map)]
      (if spec
        (let [spec-vec (if (vector? spec) spec [spec])
              query    (reduce-kv (fn [acc idx spec]
                                    (let [code-var (str "?code" idx)]
                                      (-> (update acc :selectOne conj code-var)
                                          (update :where conj [spec "_fn/code" code-var]))))
                                  {:selectOne [] :where []} spec-vec)
              fn-code  (<? (dbproto/-query db query))
              fn-code' (dbfunctions/combine-fns fn-code)
              sid      (.-s flake)
              o        (.-o flake)
              ctx      {:db      db
                        :sid     sid
                        :pid     pid
                        :o       o
                        :flakes  [flake]
                        :auth_id auth_id
                        :state   (atom {:stack   []
                                        :credits 10000000
                                        :spent   0})}
              f-meta   (<? (dbfunctions/parse-fn db fn-code' "predSpec" nil))
              res      (f-meta ctx)
              res*     (if (async-util/channel? res) (<? res) res)]
          (if res*
            true
            (throw (ex-info (str (if specDoc (str specDoc " Value: " o) (str "Object " o " does not conform to the spec for predicate: " (:name pred-map))))
                            {:status 400
                             :error  :db/invalid-tx})))) true))))


(defn validate-predicate-spec
  [db flakes auth_id block-instant]
  (go-try
    (let [true-flakes      (clojure.set/select #(.-op %) flakes)
          ;; First, we check _predicate/spec -> this runs for every single flake that is being added and has a spec.
          predSpecValid?   (loop [[flake & r] true-flakes]
                             (let [res (<? (valid-predicate-spec-flake? flake db auth_id))]
                               (if (and res r)
                                 (recur r) res)))
          ;; If _predicate/spec checks out, we test _predicate/txSpec -> This runs once with all of the flakes, both true and false,
          ;; within a transaction. This is mainly useful for checking that the sum of values for add flakes = the sum of values for
          ;; remove flakes.
          allTxPreds       (->> (map #(.-p %) flakes) (set))
          predTxSpecValid? (loop [[pred & r] allTxPreds]
                             (let [pred-map  (->> db :schema :pred ((fn [coll]
                                                                      (get coll pred))))
                                   txSpec    (:txSpec pred-map)
                                   txSpecDoc (:txSpecDoc pred-map)
                                   pred-name (:name pred-map)
                                   res       (if txSpec
                                               (let [spec-vec (if (vector? txSpec) txSpec [txSpec])
                                                     query    (reduce-kv (fn [acc idx spec]
                                                                           (let [code-var (str "?code" idx)]
                                                                             (-> (update acc :selectOne conj code-var)
                                                                                 (update :where conj [spec "_fn/code" code-var]))))
                                                                         {:selectOne [] :where []} spec-vec)
                                                     fn-code  (<? (dbproto/-query db query))
                                                     fn-code' (dbfunctions/combine-fns fn-code)
                                                     ctx      {:db      db
                                                               :pid     pred
                                                               :instant block-instant
                                                               :flakes  (filterv #(= (.-p %) pred) flakes)
                                                               :auth_id auth_id
                                                               :state   (atom {:stack   []
                                                                               :credits 10000000
                                                                               :spent   0})}
                                                     f-meta   (<? (dbfunctions/parse-fn db fn-code' "predSpec" nil))]
                                                 (f-meta ctx)) true)
                                   res*      (if (async-util/channel? res) (<? res) res)
                                   res''     (if res* res* (throw (ex-info (str "The predicate " pred-name " does not conform to spec. " txSpecDoc)
                                                                           {:status 400
                                                                            :error  :db/invalid-tx})))]
                               (if (and res'' r)
                                 (recur r) res'')))] true)))

(defn validate-permissions
  "Validates transaction based on the state of the new database."
  [db-before candidate-db flakes tx-permissions]
  (go-try
    (let [no-filter? (true? (:root? tx-permissions))]
      (if no-filter?
        ;; everything allowed, just return
        true
        ;; go through each statement and check
        (loop [[^Flake flake & r] flakes]
          (when (> (.-s flake) const/$maxSystemPredicates)
            (when-not (if (.-op flake)
                        (<? (perm-validate/allow-flake? candidate-db flake tx-permissions))
                        (<? (perm-validate/allow-flake? db-before flake tx-permissions)))
              (throw (ex-info (format "Insufficient permissions for predicate: %s within collection: %s."
                                      (dbproto/-p-prop db-before :name (.-p flake))
                                      (dbproto/-c-prop db-before :name (flake/sid->cid (.-s flake))))
                              {:status 400
                               :error  :db/write-permission}))))
          (if r
            (recur r)
            true))))))