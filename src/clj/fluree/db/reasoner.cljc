(ns fluree.db.reasoner
  (:require [clojure.string :as str]
            [fluree.db.flake :as flake]
            [fluree.db.json-ld.iri :as iri]
            [fluree.db.util.core :as util :refer [try* catch*]]
            [fluree.db.reasoner.util :refer [parse-rules-graph]]
            [fluree.db.util.log :as log]
            [fluree.db.flake.flake-db :as flake-db  :refer [db?]]
            [fluree.db.flake.transact :as flake.transact]
            [fluree.db.util.async :refer [go-try <?]]
            [fluree.db.reasoner.resolve :as resolve]
            [fluree.db.fuel :as fuel]
            [fluree.db.constants :as const]
            [fluree.json-ld :as json-ld]
            [fluree.db.query.fql.parse :as q-parse]
            [fluree.db.reasoner.owl-datalog :as owl-datalog]
            [fluree.db.reasoner.graph :refer [task-queue add-rule-dependencies]]))

#?(:clj (set! *warn-on-reflection* true))

(defn schedule
  "Returns list of rule @id values in the order they should be run.

  If optional result-summary is provided, rules that don't need to be
  re-run will be filtered out.

  A result summary is list/set of the rule dependency patterns which
  match newly created Flakes from the last run. When the result-summary
  is empty, no rules will be left to run, but based on the dependencies
  it is possible no rules will be left to run even if the result-summary
  is non-empty"
  ([rules]
   (task-queue rules))
  ([rules result-summary]
   (task-queue rules result-summary)))

(defn reasoner-insert
  "When triples from rules require explicit inserts, returns flakes."
  [db fuel-tracker rule-id insert-smt]
  (go-try
    (let [tx-state (-> (flake.transact/->tx-state :db db, :reasoned-from-iri rule-id)
                       (assoc :stage-update? true))
          [db* new-flakes] (<? (flake.transact/generate-flakes db fuel-tracker insert-smt tx-state))]
      (<? (flake.transact/final-db db* new-flakes tx-state)))))

(defn reasoner-stage
  [db fuel-tracker rule-id full-rule]
  (go-try
    (let [tx-state   (flake.transact/->tx-state :db db, :reasoned-from-iri rule-id)
          parsed-txn (:rule-parsed full-rule)]
      (when-not (:where parsed-txn)
        (throw (ex-info (str "Unable to execute reasoner rule transaction due to format error: " (:rule full-rule))
                        {:status 400 :error :db/invalid-transaction})))
      (<? (flake.transact/generate-flakes db fuel-tracker parsed-txn tx-state)))))

(defn filter-same-as-trans
  "Note - this remove 'self' from sameAs transitive
  rule. This would not be necessary if the filter function
  allowed you to filter 'o' values that are equal to 's' values
  but until that works. this addresses the issue."
  [rule-id new-flakes]
  (if (str/starts-with? rule-id (str const/iri-owl:sameAs "(trans)"))
    (reduce
      (fn [acc new-flake]
        (if (= (flake/s new-flake) (flake/o new-flake))
          acc
          (conj acc new-flake)))
      (empty new-flakes)
      new-flakes)
    new-flakes))

(defn execute-reasoner-rule
  [db rule-id reasoning-rules fuel-tracker tx-state]
  (go-try
    (let [[db reasoner-flakes] (<? (reasoner-stage db fuel-tracker rule-id (get reasoning-rules rule-id)))
          tx-state*        (assoc tx-state :stage-update? true)
          reasoner-flakes* (filter-same-as-trans rule-id reasoner-flakes)]
      (log/debug "reasoner flakes: " rule-id reasoner-flakes*)
      ;; returns map of :db-after, :add, :remove - but for reasoning we only support adds, so remove should be empty
      (<? (flake.transact/final-db db reasoner-flakes* tx-state*)))))

(defn execute-reasoner
  "Executes the reasoner on the staged db-after and returns the updated db-after."
  [db reasoning-rules fuel-tracker reasoner-max tx-state]
  (go-try
    (let [rule-schedule (schedule reasoning-rules)]
      (log/debug "reasoning schedule: " rule-schedule)
      (if (seq rule-schedule)
        (loop [[rule-id & r] rule-schedule
               reasoned-flakes nil ;; Note these are in an AVL set in with spot comparator
               reasoned-db     db
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
                  maxed?              (when reasoner-max
                                        (= (:iterations summary*) reasoner-max))]

              (log/debug "Total reasoned flakes:" (:total-flakes summary*))
              "completed in:" (:iterations summary*) "iteration(s)."

              (if (and new-flakes? (not maxed?))
                (recur rule-schedule nil reasoned-db summary*)
                (do
                  (when (and maxed? new-flakes?)
                    (log/warn (str "Reasoner reached max iterations: " reasoner-max
                                   ". Returning db reasoned thus far.")))
                  reasoned-db)))))
        db))))

(defmulti rules-from-graph (fn [method _inserts _graph]
                             method))

(defmethod rules-from-graph :datalog
  [_ _ graph]
  (reduce
    (fn [acc rule]
      (if (map? rule)
        (let [id   (:id rule)
              rule (util/get-first-value rule const/iri-rule)]
          (if rule
            (conj acc [(or id (iri/new-blank-node-id)) rule])
            acc))
        ;; else already in two-tuple form
        (conj acc rule)))
    []
    graph))

(defmethod rules-from-graph :owl2rl
  [_ inserts graph]
  (let []
    (log/debug "Reasoner - source OWL rules: " graph)
    (owl-datalog/owl->datalog inserts graph)))

(defn all-rules
  "Gets all relevant rules for the specified methods from the
  supplied rules graph or from the db if no graph is supplied."
  [methods db inserts graph-or-db]
  (go-try
    (let [rules-db        (cond
                            (nil? graph-or-db) db
                            (db? graph-or-db) graph-or-db)
          supplied-rules* (when-not rules-db
                            (try*
                              (parse-rules-graph graph-or-db)
                              (catch* e
                                      (log/error "Error parsing supplied rules graph:" e)
                                      (throw e))))]
      (loop [[method & r] methods
             rules []]
        (if method
          (let [rules-graph* (or supplied-rules*
                                 (<? (resolve/rules-from-db rules-db method)))
                rules*       (rules-from-graph method inserts rules-graph*)]
            (recur r (into rules rules*)))
          rules)))))

(defn triples->map
  "Turns triples from same subject (@id) originating from
  raw inserts that might exist in reasoning graph (e.g. owl:sameAs)
  into fluree/stage standard format."
  [id triples]
  (reduce
    (fn [acc [_ p v]]
      (update acc p (fn [ev]
                      (if ev
                        (conj ev v)
                        [v]))))
    {"@id" id}
    triples))

(defn inserts-by-rule
  "Creates fluree/stage insert statements for each individual rule that created
  triples. This is only used for raw inserts that are triggered from the reasoning
  graph (e.g. owl:sameAs)"
  [inserts]
  (reduce-kv
    (fn [acc rule-id triples]
      (let [by-subj    (group-by first triples)
            statements (reduce-kv
                         (fn [acc* id triples]
                           (conj acc* (triples->map id triples)))
                         []
                         by-subj)
            parsed     (->> statements
                            json-ld/expand
                            (q-parse/parse-triples nil nil))]
        (assoc acc rule-id {:insert parsed})))
    {}
    inserts))

(defn process-inserts
  "Processes any raw inserts that originate from the reasoning
  graph (e.g. owl:sameAs statements)"
  [db fuel-tracker inserts]
  (go-try
    (let [by-rule (inserts-by-rule inserts)]
      (loop [[[rule-id insert] & r] by-rule
             db* db]
        (if rule-id
          (let [{db**   :db-after
                 flakes :add} (<? (reasoner-insert db* fuel-tracker rule-id insert))]
            (log/debug "Rule Flake insert:" rule-id "flakes:" flakes)
            (recur r db**))
          db*)))))

(defn reason
  [db methods graph-or-db {:keys [max-fuel reasoner-max]
                           :or   {reasoner-max 10} :as _opts}]
  (go-try
    (let [methods*        (set (util/sequential methods))
          fuel-tracker    (fuel/tracker max-fuel)
          db*             (update db :reasoner #(into methods* %))
          tx-state        (flake.transact/->tx-state :db db*)
          inserts         (atom nil)
          ;; TODO - rules can be processed in parallel
          raw-rules       (<? (all-rules methods* db* inserts graph-or-db))
          _               (log/debug "Reasoner - extracted rules: " raw-rules)
          reasoning-rules (-> raw-rules
                              resolve/rules->graph
                              add-rule-dependencies)
          db**            (if-let [inserts* @inserts]
                            (<? (process-inserts db* fuel-tracker inserts*))
                            db*)]
      (log/trace "Reasoner - parsed rules: " reasoning-rules)
      (if (empty? reasoning-rules)
        db**
        (<? (execute-reasoner db** reasoning-rules fuel-tracker reasoner-max tx-state))))))
