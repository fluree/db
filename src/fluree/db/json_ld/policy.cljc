(ns fluree.db.json-ld.policy
  (:require [fluree.db.dbproto :as dbproto]
            [fluree.db.util.async :refer [<? go-try]]
            [clojure.core.async :as async]
            [fluree.db.util.core :as util :refer [try* catch*]]
            [fluree.db.util.log :as log]
            [fluree.db.flake :as flake]
            [fluree.db.json-ld.policy-validate :as validate]
            [fluree.db.query.fql :refer [query]]))

#?(:clj (set! *warn-on-reflection* true))

(def restriction-properties
  #{:f/equals :f/contains})

;; TODO - update "fluree-root-rule"
(defn all-rules
  [db]
  (go-try
    ;; TODO - once supported, use context to always return :f/allow and :f/property as vectors so we don't need to coerce downstream
    (<? (query db {:select {'?s [:*
                                 {:rdf/type [:_id]}
                                 {:f/allow [:* {:f/targetRole [:_id]}]}
                                 {:f/property [:* {:f/allow [:* {:f/targetRole [:_id]}]}]}]}
                   :where  [['?s :rdf/type :f/Policy]]}))))


(defn rules-for-roles
  "Filters all rules into only those that apply to the given roles."
  [roles all-rules]
  (filter
    (fn [rule]
      ;; if a top-level (class rule) applies to any roles, can return immediately
      (let [class-rule? (some->> (get rule :f/allow)
                                 util/sequential
                                 (some #(roles (get-in % [:f/targetRole :_id]))))]
        ;; if class rules doesn't exist for roles, check if any property rules exist
        (or class-rule?
            (when-let [property-rules (get rule :f/property)]
              (some
                (fn [property-rule]
                  (some->> (get property-rule :f/allow)
                           util/sequential
                           (some #(roles (get-in % [:f/targetRole :_id])))))
                (util/sequential property-rules))))))
    all-rules))

(defn restrict-view?
  "Given a restriction (the map value of an :f/allow property),
  does it apply to view permissions?"
  [restriction]
  (= :f/view (get-in restriction [:f/action :id])))

(defn restrict-modify?
  "Given a restriction (the map value of an :f/allow property),
  does it apply to modify permissions?"
  [restriction]
  (= :f/modify (get-in restriction [:f/action :id])))

(defn restrict-rule?
  "If a restriction rule is in place that must be evaluated,
  meaning there are ':f/equals', ':f/contains', etc. restrictions."
  [restriction]
  (some restriction-properties (-> restriction keys)))

(defn compile-restriction
  "A restriction is the map value of an :f/allow property.

  Returns a function with two args - first the permissions map which contains
  the :ident and (soon) other metadata that might be used in evaluations, and
  secondly the flake being evaluated."
  [restriction]
  (let [view?          (restrict-view? restriction)
        restrict-rule? (restrict-rule? restriction)]
    ;; TODO - for now this only looks for view rules, not modify rules. Need to address modify when working on txns
    (when view?
      (if restrict-rule?
        (do
          (log/warn "Not yet enforcing conditional restriction rules as found for: " restriction)
          ;; return two-tuple of [async? fn]
          [false (constantly false)])
        [false (constantly true)]))))

(defn subids
  "Returns a vector of subids from the input collection as a single result async chan.
  If any exception occurs during resolution, returns the error immediately."
  [db subjects]
  (async/go-loop [[next-sid & r] (map #(dbproto/-subid db %) subjects)
                  acc []]
    (if next-sid
      (let [next-res (async/<! next-sid)]
        (if (util/exception? next-res)
          next-res
          (recur r (conj acc (async/<! next-sid)))))
      acc)))


(defn compile-property-rules
  "Returns a map with property ids as keys with two-tuple value of async? + policy function.

  If function is async downstream the value will need to be retrieved downstream."
  [db rule]
  (go-try
    (let [property-rules (util/sequential rule)]
      (loop [[prop-rule & r] property-rules
             acc {}]
        (if prop-rule
          (let [prop-sid      (<? (dbproto/-subid db (get-in prop-rule [:f/path :id])))
                equals-rule   (some->> prop-rule :f/allow :f/equals (map :id))
                contains-rule (some->> prop-rule :f/allow :f/contains (map :id))
                ;; TODO - need to make sure only :f/view value for :f/action are included if for reads - could be done when selecting rules by filtering non :f/view
                ident-first?  (= :f/$identity (or (first equals-rule)
                                                  (first contains-rule)))
                path          (if ident-first?
                                (rest equals-rule)
                                equals-rule)
                path-pids     (<? (subids db path))
                f             (cond
                                equals-rule
                                (fn [{:keys [permissions] :as db} flake]
                                  (go-try
                                    (let [path-val (or (get @(:cache permissions) equals-rule)
                                                       (<? (validate/resolve-equals-rule db path-pids equals-rule)))]
                                      (= (flake/s flake) path-val))))

                                contains-rule
                                (fn [{:keys [permisssions] :as db} flake]
                                  (go-try
                                    (let [path-val (or (get @(:cache permisssions) equals-rule)
                                                       (<? (validate/resolve-contains-rule db path-pids equals-rule)))]
                                      (= (flake/s flake) path-val)))))]
            (when-not ident-first?
              (log/warn (str "Policy f:equals and f:contains only supports equals paths that start with f:$identity currently. Provided: "
                             equals-rule ". Ignoring.")))
            ;; TODO - if multiple rules target the same path we need to concatenate them and should use an 'or' condition
            (when (get acc prop-sid)
              (log/warn (str "Multiple policy rules in the same class target the same property: "
                             (get prop-rule :f/path) ". Only the last one encountered will be utilized.")))
            (recur r (assoc acc prop-sid [true f])))
          acc)))))

(defn compile-class-rule
  [db rule classes]
  (go-try
    (let [class-sids            (<? (subids db classes))
          restrictions          (get rule :f/allow)
          default-restrictions  (if (sequential? restrictions)
                                  (let [all-restrictions (map compile-restriction restrictions)]
                                    ;; return two-tuple of [async? fn]
                                    [false (fn [db flake]
                                             (some (fn [restriction-fn]
                                                     (restriction-fn db flake)) all-restrictions))])
                                  (compile-restriction restrictions))
          property-restrictions (when-let [prop-rules (:f/property rule)]
                                  (<? (compile-property-rules db prop-rules)))
          all-restrictions      (assoc property-restrictions :default default-restrictions)]
      ;; for each class targeted by the rule, map to each compiled fn
      (reduce
        (fn [acc class-sid]
          (assoc acc class-sid all-restrictions))
        {} class-sids))))


(defn compile-node-rule
  [db rule nodes]
  (go-try
    (let [node-sids             (<? (subids db nodes))
          restrictions          (get rule :f/allow)
          compiled-restrictions (if (sequential? restrictions)
                                  (let [all-restrictions (map compile-restriction restrictions)]
                                    (fn [x]
                                      (some (fn [restriction-fn]
                                              (restriction-fn x)) all-restrictions)))
                                  (compile-restriction restrictions))]
      (when (:f/property rule)
        (log/warn "Currently, property based restrictions are not yet enforced. Found for nodes: " nodes))
      ;; for each class targeted by the rule, map to each compiled fn
      (if (and (= [:f/allNodes] nodes)
               (nil? (->> node-sids first (get compiled-restrictions))))
        {:root? true}
        (reduce
          (fn [acc node-sid]
            (assoc acc node-sid compiled-restrictions))
          {} node-sids)))))


(defn compile-rule
  [db rule]
  (go-try
    (let [classes (some->> rule :f/targetClass util/sequential (mapv :id))
          nodes   (some->> rule :f/targetNode util/sequential (mapv :id))]
      (cond
        classes {:class (<? (compile-class-rule db rule classes))}
        nodes {:node (<? (compile-node-rule db rule nodes))}))))


(defn compile-rules
  "Compiles rules into a fn that returns truthy if, when given a flake, is allowed."
  [db rules]
  ;; TODO - if multiple rules target the same class, we need to 'or' the rules together.
  (->> rules
       (map #(compile-rule db %))
       async/merge
       (async/into [])))


(defn find-rules
  "Returns all the rules for the provided roles as compiled functions"
  [db {:keys [roles]}]
  ;; TODO - no caching is being done here yet, need to implement for at least all-rules lookup
  (go-try
    (let [all-rules (<? (all-rules db))]
      (rules-for-roles roles all-rules))))


(defn wrap-policy
  "Given a db object, wraps specified policy permissions"
  [db identity role credential]
  ;; TODO - not yet paying attention to verifiable credentials that are present
  (async/go
    (try*
      (let [ident-sid      (<? (dbproto/-subid db identity))
            role-sids      (if (sequential? role)
                             (->> (<? (subids db role))
                                  (into #{}))
                             #{(<? (dbproto/-subid db role))})
            permissions    {:ident ident-sid
                            :roles role-sids
                            :root? (empty? role-sids)}
            rules          (<? (find-rules db permissions))
            compiled-rules (->> (<? (compile-rules db rules))
                                (apply merge))
            root-rule?     (= compiled-rules
                              {:node {:root? true}})
            permissions*   (cond-> (assoc permissions :view compiled-rules)
                                   root-rule? (assoc :root? true))]
        (assoc db :permissions permissions*))
      (catch* e
              (if (= :db/invalid-query (:error (ex-data e)))
                (throw (ex-info (str "There are no Fluree rules in the db, a policy-driven database cannot be retrieved. "
                                     "If you have created rules, make sure they are of @type f:Rule.")
                                {:status 400
                                 :error  :db/invalid-policy}))
                (throw e))))))