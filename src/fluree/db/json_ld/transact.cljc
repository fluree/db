(ns fluree.db.json-ld.transact
  (:require [clojure.core.async :as async :refer [alts! go]]
            [fluree.db.constants :as const]
            [fluree.db.dbproto :as dbproto]
            [fluree.db.flake :as flake]
            [fluree.db.json-ld.branch :as branch]
            [fluree.db.json-ld.commit-data :as commit-data]
            [fluree.db.json-ld.ledger :as jld-ledger]
            [fluree.db.json-ld.policy :as perm]
            [fluree.db.json-ld.shacl :as shacl]
            [fluree.db.json-ld.vocab :as vocab]
            [fluree.db.ledger.proto :as ledger-proto]
            [fluree.db.policy.enforce-tx :as policy]
            [fluree.db.query.exec :as exec]
            [fluree.db.query.exec.update :as update]
            [fluree.db.query.exec.where :as where]
            [fluree.db.query.fql.parse :as q-parse]
            [fluree.db.query.range :as query-range]
            [fluree.db.util.async :refer [<? go-try]]
            [fluree.db.util.core :as util]
            [fluree.json-ld :as json-ld]
            [fluree.store.core :as store]))

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
                {:keys [new? classes shacl]} (get subj-mods' sid)]
            (when shacl
              (let [shacl*    (mapv (fn [shape]
                                      (update shape :validated-properties (fnil into #{})
                                              (get shape->validated-properties (:id shape))))
                                    shacl)
                    s-flakes* (if new?
                                s-flakes
                                (<? (query-range/index-range root-db :spot = [sid])))]
                (<? (shacl/validate-target shacl* root-db s-flakes*))))
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
                  (<? (shacl/validate-target shacl root-db flakes))
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

(defn next-id
  "Generate the next subject id - `counter-key` is either :last-pid or :last-sid."
  [iri->sid counter-key iri]
  (let [iri->sid* (swap! iri->sid
                         (fn [iri->sid]
                           (if (get iri->sid iri)
                             iri->sid
                             (-> iri->sid
                                 (assoc iri (get iri->sid counter-key))
                                 (update counter-key inc)))))]
    (get iri->sid* iri)))

(defn ->tx-state
  [db txn-id author-did]
  (let [{:keys [schema branch ledger policy], db-t :t} db
        iri->sid (atom {:last-pid (jld-ledger/last-pid db)
                        :last-sid (jld-ledger/last-sid db)})
        commit-t  (-> (ledger-proto/-status ledger branch) branch/latest-commit-t)
        t         (-> commit-t inc -) ;; commit-t is always positive, need to make negative for internal indexing
        db-before (dbproto/-rootdb db)]
    {:db-before                db-before
     :txn-id                   txn-id
     :author-did               author-did
     :policy                   policy
     :stage-update?            (= t db-t) ;; if a previously staged db is getting updated again before committed
     :t                        t
     :iri->sid                 iri->sid
     :next-pid                 (partial next-id iri->sid :last-pid)
     :next-sid                 (partial next-id iri->sid :last-sid)
     :iris                     (volatile! {})}))

(defn generate-flakes
  [db fuel-tracker parsed-txn tx-state]
  (go
    (let [error-ch  (async/chan)
          update-ch (->> (where/search db parsed-txn fuel-tracker error-ch)
                         (update/modify db parsed-txn tx-state fuel-tracker error-ch)
                         (exec/into-flakeset fuel-tracker error-ch))]
      (async/alt!
        error-ch ([e] e)
        update-ch ([flakes] flakes)))))

(defn subject-mods
  [new-flakes db-before]
  (go-try
    (let [has-target-objects-of-shapes (shacl/has-target-objects-of-rule? db-before)]
      (loop [[s-flakes & r] (partition-by flake/s new-flakes)
             subj-mods      {}]
        (if s-flakes
          (let [sid            (flake/s (first s-flakes))
                new-subject?   (first (filterv #(= const/$xsd:anyURI (flake/p %)) s-flakes))
                new-classes    (->> s-flakes
                                    (filterv #(= const/$rdf:type (flake/p %)))
                                    (mapv flake/o))
                classes        (if new-subject?
                                 new-classes
                                 (into new-classes
                                       (<? (query-range/index-range db-before :spot = [sid const/$rdf:type]
                                                                    {:flake-xf (map flake/o)}))))
                class-shapes   (<? (shacl/class-shapes db-before classes))
                ;; these target objects in s-flakes
                pid->ref-flakes (when has-target-objects-of-shapes
                                  (->> s-flakes
                                       (filterv #(= const/$xsd:anyURI (flake/dt %)))
                                       (group-by flake/p)))
                o-pred-shapes   (when (seq pid->ref-flakes)
                                  (<? (shacl/targetobject-shapes db-before (keys pid->ref-flakes))))
                ;; these target subjects in s-flakes
                referring-pids (when has-target-objects-of-shapes
                                 (<? (query-range/index-range db-before :opst = [sid]
                                                              {:flake-xf (map flake/p)})))
                s-pred-shapes  (when (seq referring-pids)
                                 (<? (shacl/targetobject-shapes db-before referring-pids)))
                shacl-shapes   (into class-shapes s-pred-shapes)]
            (recur r (reduce
                       (fn [subj-mods o-pred-shape]
                         (let [target-os (->> (get pid->ref-flakes (:target-objects-of o-pred-shape))
                                              (mapv flake/o))]
                           (reduce (fn [subj-mods target-o]
                                     (update-in subj-mods [target-o :shacl] (fnil conj []) o-pred-shape))
                                   subj-mods
                                   target-os)))
                       (-> subj-mods
                           (assoc-in [sid :new?] (boolean new-subject?))
                           (update-in [sid :classes] (fnil into []) classes)
                           (update-in [sid :shacl] (fnil into []) shacl-shapes))
                       o-pred-shapes)))
          subj-mods)))))

(defn final-db
  "Returns map of all elements for a stage transaction required to create an
  updated db."
  [new-flakes {:keys [stage-update? db-before iri->sid policy t txn-id author-did] :as _tx-state}]
  (go-try
    (let [[add remove] (if stage-update?
                         (stage-update-novelty (get-in db-before [:novelty :spot]) new-flakes)
                         [new-flakes nil])

          {:keys [last-pid last-sid]} @iri->sid

          db-after (-> db-before
                       (update :txns (fnil conj []) [txn-id author-did])
                       (update :ecount assoc const/$_predicate last-pid)
                       (update :ecount assoc const/$_default last-sid)
                       (assoc :policy policy) ;; re-apply policy to db-after
                       (assoc :t t)
                       (commit-data/update-novelty add remove)
                       (commit-data/add-tt-id)
                       (vocab/hydrate-schema add))]
      {:add add, :remove remove, :db-after db-after})))

(defn flakes->final-db
  "Takes final set of proposed staged flakes and turns them into a new db value
  along with performing any final validation and policy enforcement."
  [tx-state flakes]
  (go-try
    (let [subj-mods (<? (subject-mods flakes (:db-before tx-state)))
          ;; wrap it in an atom to reuse old validate-rules and policy/allowed? unchanged
          ;; TODO: remove the atom wrapper once subj-mods is no longer shared code
          tx-state* (assoc tx-state :subj-mods (atom subj-mods))]
      (-> (final-db flakes tx-state)
          <?
          (validate-rules tx-state*)
          <?
          (policy/allowed? tx-state*)
          <?
          dbproto/-rootdb))))

(defn stage
  ([db txn parsed-opts]
   (stage db nil txn parsed-opts))
  ([db fuel-tracker txn parsed-opts]
   (go-try
     (let [{:keys [context raw-txn did]} parsed-opts
           {:keys [conn ledger]} db

           parsed-txn    (q-parse/parse-txn txn context)
           db*           (if-let [policy-identity (perm/parse-policy-identity parsed-opts context)]
                           (<? (perm/wrap-policy db policy-identity))
                           db)

           {txn-id :k}  (store/write (:store conn)
                                     (str (ledger-proto/-alias ledger) "/txn/")
                                     (json-ld/normalize-data raw-txn)
                                     {:content-address? true})

           tx-state      (->tx-state db* txn-id did)
           flakes-ch     (generate-flakes db fuel-tracker parsed-txn tx-state)
           fuel-error-ch (:error-ch fuel-tracker)
           chans         (remove nil? [fuel-error-ch flakes-ch])
           [flakes]      (alts! chans :priority true)]
       (when (util/exception? flakes)
         (throw flakes))
       (<? (flakes->final-db tx-state flakes))))))
