(ns fluree.db.json-ld.transact
  (:require [clojure.core.async :as async :refer [go]]
            [fluree.db.util.log :as log]
            [fluree.db.constants :as const]
            [fluree.db.fuel :as fuel]
            [fluree.db.json-ld.policy :as perm]
            [fluree.db.dbproto :as dbproto]
            [fluree.db.flake :as flake]
            [fluree.db.json-ld.commit-data :as commit-data]
            [fluree.db.json-ld.shacl :as shacl]
            [fluree.db.json-ld.vocab :as vocab]
            [fluree.db.policy.enforce-tx :as policy]
            [fluree.db.query.fql.parse :as q-parse]
            [fluree.db.query.exec.update :as update]
            [fluree.db.query.exec.where :as where]
            [fluree.db.query.range :as query-range]
            [fluree.db.util.async :refer [<? go-try]]
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
            (let [new-shacl? (or (contains? all-classes const/sh_NodeShape)
                                 (contains? all-classes const/sh_PropertyShape))]
              (when new-shacl?
                ;; TODO - PropertyShape class is often not specified for sh:property nodes - direct changes to those would not be caught here!
                (vocab/reset-shapes (:schema db-after)))
              staged-map)))))))

(defn validate
  [{:keys [db-after db-before mods] :as staged-map}]
  (def sm staged-map)
  (go-try
    (let [root-db (dbproto/-rootdb db-after)]
      (<? (shacl/validate! db-before root-db (vals mods)))
      staged-map)))

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
  "Generates a state map for transaction processing. When optional
  reasoned-from-IRI is provided, will mark any new flakes as reasoned from the
  provided value in the flake's metadata (.-m) as :reasoned key."
  ([db txn author-did]
   (->tx-state db txn author-did nil))

  ([db txn author-did reasoned-from-iri]
  (let [{:keys [policy], db-t :t} db

        commit-t  (-> db :commit commit-data/t)
        t         (flake/next-t commit-t)
        db-before (dbproto/-rootdb db)]
    {:db-before     db-before
     :txn           txn
     :author-did    author-did
     :policy        policy
     :stage-update? (= t db-t) ; if a previously staged db is getting updated
                               ; again before committed
     :t             t
     :reasoner-max  10 ; maximum number of reasoner iterations before exception
     :reasoned      reasoned-from-iri})))

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

(def extract-class-xf
  (comp
    (filter flake/class-flake?)
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

(defn modified-subjects
  "Returns a map of sid to s-flakes for each modified subject."
  [db flakes]
  (go-try
    (loop [[s-flakes & r] (partition-by flake/s flakes)
           sid->s-flakes  {}]
      (if s-flakes
        (let [sid             (some-> s-flakes first flake/s)
              existing-flakes (<? (query-range/index-range db :spot = [sid]))]
          (recur r (assoc sid->s-flakes sid (into (set s-flakes) existing-flakes))))
        sid->s-flakes))))

(defn final-db
  "Returns map of all elements for a stage transaction required to create an
  updated db."
  [db new-flakes {:keys [stage-update? policy t txn author-did db-before] :as _tx-state}]
  (go-try
    (let [[add remove] (if stage-update?
                         (stage-update-novelty (get-in db [:novelty :spot]) new-flakes)
                         [new-flakes nil])

          mods (<? (modified-subjects db add))

          db-after  (-> db
                        (update :staged conj [txn author-did])
                        (assoc :policy policy) ;; re-apply policy to db-after
                        (assoc :t t)
                        (commit-data/update-novelty add remove)
                        (commit-data/add-tt-id)
                        (vocab/hydrate-schema add mods))]
      {:add add :remove remove :db-after db-after :db-before db-before :mods mods})))

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
          <?
          validate
          <?
          (validate-rules tx-state*)
          <?
          (policy/allowed?)
          <?
          dbproto/-rootdb))))

(defn stage
  ([db txn parsed-opts]
   (stage db nil txn parsed-opts))
  ([db fuel-tracker txn parsed-opts]
   (go-try
     (let [{:keys [context raw-txn did]} parsed-opts

           parsed-txn (q-parse/parse-txn txn context)
           db*        (if-let [policy-identity (perm/parse-policy-identity parsed-opts context)]
                        (<? (perm/wrap-policy db policy-identity))
                        db)
           tx-state   (->tx-state db* raw-txn did)
           flakes     (<? (generate-flakes db fuel-tracker parsed-txn tx-state))]
       (<? (flakes->final-db fuel-tracker tx-state flakes))))))
