(ns fluree.db.json-ld.transact
  (:require [clojure.core.async :as async :refer [alts! go]]
            [fluree.db.util.log :as log]
            [fluree.db.conn.proto :as conn-proto]
            [fluree.db.constants :as const]
            [fluree.db.fuel :as fuel]
            [fluree.db.json-ld.policy :as perm]
            [fluree.db.dbproto :as dbproto]
            [fluree.db.flake :as flake]
            [fluree.db.json-ld.branch :as branch]
            [fluree.db.json-ld.commit-data :as commit-data]
            [fluree.db.json-ld.shacl :as shacl]
            [fluree.db.json-ld.vocab :as vocab]
            [fluree.db.ledger.proto :as ledger-proto]
            [fluree.db.policy.enforce-tx :as policy]
            [fluree.db.query.fql.parse :as q-parse]
            [fluree.db.query.exec.update :as update]
            [fluree.db.query.exec.where :as where]
            [fluree.db.query.range :as query-range]
            [fluree.db.util.async :refer [<? go-try]]
            [fluree.db.reasoner.core :as reasoner]
            [fluree.db.util.core :as util]))

#?(:clj (set! *warn-on-reflection* true))

(defn validate-rules
  [{:keys [db-after add] :as staged-map} {:keys [subj-mods] :as _tx-state}]
  (let [subj-mods' @subj-mods
        root-db    (dbproto/-rootdb db-after)
        {:keys [shape->validated-properties]} subj-mods']
    (go-try
      (loop [[s-flakes & r] (partition-by flake/s add)
             all-classes         #{}
             remaining-subj-mods subj-mods']
        (if s-flakes
          (let [sid (flake/s (first s-flakes))
                {:keys [classes shacl]} (get subj-mods' sid)]
            (when shacl
              (let [shacl*    (mapv (fn [shape]
                                      (update shape :validated-properties (fnil into #{})
                                              (get shape->validated-properties (:id shape))))
                                    shacl)
                    s-flakes* (<? (query-range/index-range root-db :spot = [sid]))]
                (<? (shacl/validate-target shacl* root-db sid s-flakes*))))
            (recur r (into all-classes classes) (dissoc remaining-subj-mods sid)))
          ;; There may be subjects who need to have rules checked due to the addition
          ;; of a reference, but the subjects themselves were not modified in this txn.
          ;; These will appear in `subj-mods` but not among the `add` flakes.
          ;; We process validation for these remaining subjects here,
          ;; after we have looped through all the `add` flakes.
          (do
            (loop [[[sid mod] & r] (dissoc remaining-subj-mods :shape->validated-properties)]
              (when sid
                (let [{:keys [shacl]} mod
                      flakes (<? (query-range/index-range root-db :spot = [sid]))]
                  (<? (shacl/validate-target shacl root-db sid flakes))
                  (recur r))))
            (let [new-shacl? (or (contains? all-classes const/$sh:NodeShape)
                                 (contains? all-classes const/$sh:PropertyShape))]
              (when new-shacl?
                ;; TODO - PropertyShape class is often not specified for sh:property nodes - direct changes to those would not be caught here!
                (vocab/reset-shapes (:schema db-after)))
              staged-map)))))))

;; TODO - can use transient! below
(defn stage-update-novelty
  "If a db is staged more than once, any retractions in a previous stage will
  get completely removed from novelty. This returns flakes that must be added and removed
  from novelty."
  [novelty-flakes new-flakes]
  (loop [[f & r] new-flakes
         adds    new-flakes
         removes (empty new-flakes)]
    (if f
      (if (true? (flake/op f))
        (recur r adds removes)
        (let [flipped (flake/flip-flake f)]
          (if (contains? novelty-flakes flipped)
            (recur r (disj adds f) (conj removes flipped))
            (recur r adds removes))))
      [(not-empty adds) (not-empty removes)])))

(defn ->tx-state
  "Generates a state map for transaction processing.
  When optional reasoned-from-IRI is provided, will mark
  any new flakes as reasoned from the provided value
  in the flake's metadata (.-m) as :reasoned key."
  [db txn-id author-did reasoned-from-IRI]
  (let [{:keys [branch ledger policy], db-t :t} db
        commit-t  (-> (ledger-proto/-status ledger branch) branch/latest-commit-t)
        t         (inc commit-t)
        db-before (dbproto/-rootdb db)]
    {:db-before     db-before
     :txn-id                   txn-id
     :author-did               author-did
     :policy        policy
     :stage-update? (= t db-t) ; if a previously staged db is getting updated
                               ; again before committed
     :t             t
     :reasoner-max  10 ;; maximum number of reasoner iterations before exception
     :reasoned      reasoned-from-IRI}))

(defn into-flakeset
  [fuel-tracker error-ch flake-ch]
  (let [flakeset (flake/sorted-set-by flake/cmp-flakes-spot)
        error-xf (halt-when util/exception?)
        flake-xf (if fuel-tracker
                   (let [track-fuel (fuel/track fuel-tracker error-ch)]
                     (comp error-xf track-fuel))
                   error-xf)]
    (async/transduce flake-xf (completing conj) flakeset flake-ch)))

(defn generate-flakes
  [db fuel-tracker parsed-txn tx-state]
  (go
    (let [error-ch  (async/chan)
          db-vol    (volatile! db)
          update-ch (->> (where/search db parsed-txn fuel-tracker error-ch)
                         (update/modify db-vol parsed-txn tx-state fuel-tracker error-ch)
                         (into-flakeset fuel-tracker error-ch))]
      (async/alt!
        error-ch ([e] e)
        update-ch ([result]
                   (if (util/exception? result)
                     result
                     [@db-vol result]))))))

(defn class-flake?
  [f]
  (-> f flake/p (= const/$rdf:type)))

(def extract-class-xf
  (comp
    (filter class-flake?)
    (map flake/o)))

(defn extract-classes
  [flakes]
  (into #{} extract-class-xf flakes))

(defn subject-mods
  [new-flakes db-before]
  (go-try
    (let [has-target-objects-of-shapes (shacl/has-target-objects-of-rule? db-before)]
      (loop [[s-flakes & r] (partition-by flake/s new-flakes)
             subj-mods      {}]
        (if s-flakes
          (let [new-classes      (extract-classes s-flakes)
                sid              (flake/s (first s-flakes))
                existing-classes (<? (query-range/index-range db-before :spot = [sid const/$rdf:type]
                                                              {:flake-xf (map flake/o)}))
                classes          (into new-classes existing-classes)
                class-shapes     (<? (shacl/class-shapes db-before classes))
                ;; these target objects in s-flakes
                pid->ref-flakes  (when has-target-objects-of-shapes
                                   (->> s-flakes
                                        (filter (fn [f]
                                                  (-> f flake/dt (= const/$xsd:anyURI))))
                                        (group-by flake/p)))
                o-pred-shapes    (when (seq pid->ref-flakes)
                                   (<? (shacl/targetobject-shapes db-before (keys pid->ref-flakes))))
                ;; these target subjects in s-flakes
                referring-pids   (when has-target-objects-of-shapes
                                   (<? (query-range/index-range db-before :opst = [sid]
                                                                {:flake-xf (map flake/p)})))
                s-pred-shapes    (when (seq referring-pids)
                                   (<? (shacl/targetobject-shapes db-before referring-pids)))
                shacl-shapes     (into class-shapes s-pred-shapes)
                subj-mods*       (-> subj-mods
                                     (update-in [sid :classes] (fnil into []) classes)
                                     (update-in [sid :shacl] (fnil into []) shacl-shapes))]
            (recur r (reduce
                       (fn [subj-mods o-pred-shape]
                         (let [target-os (->> (get pid->ref-flakes (:target-objects-of o-pred-shape))
                                              (mapv flake/o))]
                           (reduce (fn [subj-mods target-o]
                                     (update-in subj-mods [target-o :shacl] (fnil conj []) o-pred-shape))
                                   subj-mods
                                   target-os)))
                       subj-mods*
                       o-pred-shapes)))
          subj-mods)))))

(defn final-db
  "Returns map of all elements for a stage transaction required to create an
  updated db."
  [db new-flakes {:keys [stage-update? policy t txn-id author-did] :as _tx-state}]
  (let [[add remove] (if stage-update?
                       (stage-update-novelty (get-in db [:novelty :spot]) new-flakes)
                       [new-flakes nil])
        db-after  (-> db
                      (update :txns (fnil conj []) [txn-id author-did])
                      (assoc :policy policy) ;; re-apply policy to db-after
                      (assoc :t t)
                      (commit-data/update-novelty add remove)
                      (commit-data/add-tt-id)
                      (vocab/hydrate-schema add))]
    {:add add :remove remove :db-after db-after}))

(defn reasoner-stage
  [db fuel-tracker txn-id author-did rule-id full-rule]
  (go-try
    (let [tx-state      (->tx-state db txn-id author-did rule-id)
          parsed-txn    (:rule-parsed full-rule)
          _             (when-not (:where parsed-txn)
                          (throw (ex-info (str "Unable to execute reasoner rule transaction due to format error: " (:rule full-rule))
                                          {:status 400 :error :db/invalid-transaction})))

          flakes-ch     (generate-flakes db fuel-tracker parsed-txn tx-state)
          fuel-error-ch (:error-ch fuel-tracker)
          chans         (remove nil? [fuel-error-ch flakes-ch])
          [flakes] (alts! chans :priority true)]
      (when (util/exception? flakes)
        (throw flakes))
      flakes)))

(defn execute-reasoner-rule
  [db rule-id reasoning-rules fuel-tracker {:keys [txn-id author-did] :as tx-state}]
  (go-try
    (let [[db reasoner-flakes] (<? (reasoner-stage db fuel-tracker txn-id author-did rule-id (get reasoning-rules rule-id)))
          tx-state* (assoc tx-state :stage-update? true
                                    :db-before db)]
      (log/debug "reasoner flakes: " rule-id reasoner-flakes)
      ;; returns map of :db-after, :add, :remove - but for reasoning we only support adds, so remove should be empty
      (final-db db reasoner-flakes tx-state*))))

(defn execute-reasoner
  "Executes the reasoner on the staged db-after and returns the updated db-after."
  [{:keys [reasoner] :as db-after} fuel-tracker {:keys [reasoner-max] :as tx-state}]
  (go-try
    (if reasoner
      (let [reasoning-rules (<? (reasoner/rules-graph db-after))
            rule-schedule   (reasoner/schedule reasoning-rules)]
        (log/debug "reasoning rules: " reasoning-rules)
        (log/debug "reasoning schedule: " rule-schedule)
        (if (seq rule-schedule)
          (loop [[rule-id & r] rule-schedule
                 reasoned-flakes nil ;; Note these are in an AVL set in with spot comparator
                 reasoned-db     db-after
                 summary         {:iterations      0 ;; holds summary across all runs
                                  :reasoned-flakes []
                                  :total-flakes    0}]
            (if rule-id
              (let [{:keys [db-after add]} (<? (execute-reasoner-rule reasoned-db rule-id reasoning-rules fuel-tracker tx-state))]
                (log/debug "executed reasoning rule: " rule-id)
                (log/trace "reasoning rule: " rule-id "produced flakes:" add)
                (recur r
                       (if reasoned-flakes
                         (into reasoned-flakes add)
                         add)
                       db-after
                       summary))
              (let [all-reasoned-flakes (into reasoned-flakes (:reasoned-flakes summary))
                    summary*            {:iterations      (-> summary :iterations inc)
                                         :reasoned-flakes all-reasoned-flakes
                                         :total-flakes    (count all-reasoned-flakes)}
                    new-flakes?         (> (:total-flakes summary*)
                                           (:total-flakes summary))
                    ;reasoning-rules* (<? (convert-rules-dependency-flakes-to-SIDs reasoned-db reasoning-rules))
                    ;rules* (select-keys reasoning-rules rule-schedule) ;; reduce possible rules to only the rules that were run in last iteration
                    ;rule-schedule* (reasoner/schedule reasoning-rules (rule-dependency-fn reasoning-rules reasoned-flakes))
                    ]
                (log/debug "completed reasoning iteration number: " (:iterations summary*)
                           "Total reasoned flakes:" (:total-flakes summary*))
                (if (and new-flakes?
                         (< (:iterations summary*) reasoner-max))
                  (recur rule-schedule nil reasoned-db summary*)
                  reasoned-db))))
          db-after))
      db-after)))

(defn flakes->final-db
  "Takes final set of proposed staged flakes and turns them into a new db value
  along with performing any final validation and policy enforcement."
  [fuel-tracker tx-state [db flakes]]
  (go-try
    (let [subj-mods (<? (subject-mods flakes (:db-before tx-state)))
          ;; wrap it in an atom to reuse old validate-rules and policy/allowed? unchanged
          ;; TODO: remove the atom wrapper once subj-mods is no longer shared code
          tx-state* (assoc tx-state :subj-mods (atom subj-mods))]
      (-> (final-db db flakes tx-state)
          (validate-rules tx-state*)
          <?
          (policy/allowed? tx-state*)
          <?
          (execute-reasoner fuel-tracker tx-state*)
          <?
          dbproto/-rootdb))))

(defn stage
  ([db txn parsed-opts]
   (stage db nil txn parsed-opts))
  ([{:keys [conn ledger] :as db} fuel-tracker txn parsed-opts]
   (go-try
     (let [{:keys [context raw-txn did]} parsed-opts

           parsed-txn (q-parse/parse-txn txn context)
           db*        (if-let [policy-identity (perm/parse-policy-identity parsed-opts context)]
                        (<? (perm/wrap-policy db policy-identity))
                        db)

           {txn-id :address}
           (<? (conn-proto/-txn-write conn ledger raw-txn))

           tx-state (->tx-state db* txn-id did nil)
           flakes   (<? (generate-flakes db fuel-tracker parsed-txn tx-state))]
       (<? (flakes->final-db fuel-tracker tx-state flakes))))))
