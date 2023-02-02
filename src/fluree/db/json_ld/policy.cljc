(ns fluree.db.json-ld.policy
  (:require [clojure.core.async :as async]
            [fluree.db.util.async :refer [<? go-try]]
            [fluree.db.dbproto :as dbproto]
            [fluree.db.json-ld.policy-validate :as validate]
            [fluree.db.query.fql :refer [query]]
            [fluree.db.util.core :as util :refer [try* catch*]]
            [fluree.db.util.log :as log]))

#?(:clj (set! *warn-on-reflection* true))

;; if these keys exist in the policy's :f/allow definition, there exist specific logic rules
;; that need to get enforced (e.g. as opposed to generically allowing an entire Class, members
;; will need to get evaluated for specific criteria)
(def restriction-properties
  #{:f/equals :f/contains})

;; These special IRIs (today only one) get replaced with an actual value in the context of the
;; request.
;;  - :f/$identity - gets replaced with the subject ID of the identity (DID) that signed
;;                   the particular request
(def special-meaning-properties
  #{:f/$identity})


(defn restricted-allow-rule?
  "Returns true if an allow rule contains restrictions (e.g. :f/equals or other restriction properties)."
  [allow-rule]
  (->> (keys allow-rule)
       (some restriction-properties)))

(defn all-policies
  [db]
  (go-try
    ;; TODO - once supported, use context to always return :f/allow and :f/property as vectors so we don't need to coerce downstream
    (<? (query db {:select {'?s [:*
                                 {:rdf/type [:_id]}
                                 {:f/allow [:* {:f/targetRole [:_id]}]}
                                 {:f/property [:* {:f/allow [:* {:f/targetRole [:_id]}]}]}]}
                   :where  [['?s :rdf/type :f/Policy]]}))))


(defn policies-for-roles*
  "Filters all rules into only those that apply to the given roles."
  [roles all-policies]
  (keep
    (fn [policy]
      (let [class-policies (->> (get policy :f/allow)
                                util/sequential
                                (filter #(roles (get-in % [:f/targetRole :_id])))
                                not-empty)
            prop-policies  (when-let [all-prop-policies (get policy :f/property)]
                             ;; each explicit property can have multiple :f/allow targeting different roles
                             ;; We only want :f/allow that target provided roles, and only want properties
                             ;; returned tht contain at least one relevant :f/allow
                             (->> all-prop-policies
                                  util/sequential
                                  (filter
                                    (fn [prop-policy]
                                      (let [roles-policies (->> (get prop-policy :f/allow)
                                                                util/sequential
                                                                (filter #(roles (get-in % [:f/targetRole :_id])))
                                                                not-empty)]
                                        (when roles-policies
                                          (assoc prop-policy :f/allow roles-policies)))))
                                  not-empty))]
        (when (or class-policies prop-policies)
          (cond-> policy
                  class-policies (assoc :f/allow class-policies)
                  prop-policies (assoc :f/property prop-policies)))))
    all-policies))

(defn policies-for-roles
  "Returns all the rules for the provided roles as compiled functions"
  [db {:keys [roles]}]
  ;; TODO - no caching is being done here yet, need to implement for at least all-rules lookup
  (go-try
    (let [all-rules (<? (all-policies db))]
      (policies-for-roles* roles all-rules))))

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

(defn ident-first?
  "Returns true if first part of a comparison rule (e.g. :f/equals or :f/contains)
  is the special property :f/$identity"
  [rule-def]
  (= :f/$identity (first rule-def)))


(defn first-special-property
  "Equality paths will likely start out with a special property like :f/$identity
  which gets replaced with the verified user's identity during policy enforcement.

  If the property path starts with one of the keywords, it is returned - else nil.

  We expect to support additional keywords, for example, to extract a field from a
  validated and authorized verifiable credential."
  [prop-path]
  (special-meaning-properties (first prop-path)))

(defn- condense-property-path
  "Property paths from the original rule query come back as nested objects with :id keys.
  We just want a vector of those values."
  [property-path]
  (map :id property-path))

(defn property-path
  "For comparison rules that use a property path (e.g. :f/equals or :f/contains)
  returns the property path pids (excluding the special :f/$identity property if used).
  in vector form."
  [db rule-type rule]
  (go-try
    (let [property-path    (->> (get rule rule-type) condense-property-path)
          special-property (first-special-property property-path)
          ;; convert path to property-ids so we don't need to lookup keywords with each fn call
          property-ids     (->> (if special-property
                                  (rest property-path)
                                  property-path)
                                (subids db)
                                <?)]
      (if special-property
        (into [special-property] property-ids)
        property-ids))))


(defmulti compile-allow-rule-fn
          "Defined allow rules compile into different functions that accept both a db
          and flake argument, and return truthy if flake is allowed. Different parse rules
          currently supported are listed with their respective defmethod keyword dispatch
          values.

          Rule parsing returns two-tuple of [async? fn], where async? is a boolean indicating
          if the fn will return an async chan which will require a take (<!) to get the value."
          (fn [_ rule]
            (cond
              (contains? rule :f/equals) :f/equals
              (contains? rule :f/contains) :f/contains
              :else ::unrestricted-rule)))

(defmethod compile-allow-rule-fn :f/equals
  [db rule]
  (go-try
    (let [resolved-path (<? (property-path db :f/equals rule))]
      (validate/generate-equals-fn rule resolved-path))))

(defmethod compile-allow-rule-fn :f/contains
  [db rule]
  ;; TODO
  (go-try
    (throw (ex-info ":f/contains not yet implemented!" {}))))

(defmethod compile-allow-rule-fn ::unrestricted-rule
  [db rule]
  ;; there are no conditions on the rule, which means explicitly allow
  (go-try
    [false (constantly true)]))


(defn compile-property-policies
  "Returns a map with property ids as keys with two-tuple value of async? + policy function.

  If function is async downstream the value will need to be retrieved downstream."
  [db policy]
  (go-try
    (let [property-policies (util/sequential policy)]
      (loop [[prop-policy & r] property-policies
             acc {}]
        (if prop-policy
          (let [allow-spec (:f/allow prop-policy)
                fn-tuple   (if (sequential? allow-spec)
                             (do
                               (log/warn (str "Multiple role policies for same property is not currently allowed. Using only first "
                                              "allow specification in policy: " prop-policy "."))
                               ;; TODO - Multiple conditions existing for a single role/property is probably a valid use case but uncommon.
                               ;; TODO --- they should be treated as an -OR- condition and could be wrapped into a single fn.
                               ;; TODO --- the wrapping fn would have to look for async fns and use <?/<! takes (and itself be async as applicable)
                               (<? (compile-allow-rule-fn db (first allow-spec))))
                             (<? (compile-allow-rule-fn db allow-spec))) ;; returns two-tuple of [async? validation-fn]
                prop-sid   (<? (dbproto/-subid db (get-in prop-policy [:f/path :id])))]
            ;; TODO - if multiple rules target the same path we need to concatenate them and should use an 'or' condition
            (when (get acc prop-sid)
              (log/warn (str "Multiple policy rules in the same class target the same property: "
                             (get prop-policy :f/path) ". Only the last one encountered will be utilized.")))
            (recur r (assoc acc prop-sid fn-tuple)))
          acc)))))


(defn compile-allow-rule
  "Compiles an allow rule, which will be associated with one or more actions.

  Adds a compiled rule function (takes two args - db + flake being evaluated) along with async? flag

  Returns a map of modified rule in a map where each key is the actions where the rule must be evaluated.

  e.g. input allow-rule:
  {:id ...
   :f/targetRole ...
   :f/action [{:id :f/view}, {:id :f/modify}]
   :f/equals ... }

   Returns (note, two actions defined - so same map returned keyed by each respective action)
   {:f/view   {:id ...
               :f/fn [true <compiled function here!>]
               :f/targetRole ...
               :f/equals ... }
    :f/modify {:id ...
               :f/fn [true <compiled function here!>]
               :f/targetRole ...
               :f/equals ... }"
  [db policy-key-seqs allow-rule]
  (go-try
    (let [fn-tuple    (<? (compile-allow-rule-fn db allow-rule))
          actions     (->> allow-rule :f/action util/sequential (map :id))
          allow-rule* (-> allow-rule
                          ;; remove :f/action as we end up keying the rule *per-action* in the final policy map, don't want confusion if this hangs around that it is used
                          (dissoc :f/action)
                          ;; associate our compiled function to the existing allow-rule map
                          (assoc :function fn-tuple))]
      (for [policy-key-seq policy-key-seqs
            action         actions]
        ;; for every policy-key-seqs (key sequence that can be used with (assoc-in m <ks-here> ...))
        ;; prepend key-sequence with the policy rule's action(s)
        (let [ks* (into [action] policy-key-seq)]
          ;; return two-tuple of [full-key-seq updated-allow-rule-map]
          [ks* allow-rule*])))))

(defn compile-prop-policy
  [db default-key-seqs prop-policy]
  (go-try
    (let [allow-rule  (:f/allow prop-policy)
          fn-tuple    (if (sequential? allow-rule)
                        (do
                          (log/warn (str "Multiple role policies for same property is not currently allowed. Using only first "
                                         "allow specification in policy: " prop-policy "."))
                          ;; TODO - Multiple conditions existing for a single role/property is probably a valid use case but uncommon.
                          ;; TODO --- they should be treated as an -OR- condition and could be wrapped into a single fn.
                          ;; TODO --- the wrapping fn would have to look for async fns and use <?/<! takes (and itself be async as applicable)
                          (<? (compile-allow-rule-fn db (first allow-rule))))
                        (<? (compile-allow-rule-fn db allow-rule))) ;; returns two-tuple of [async? validation-fn]
          _           (when-not (get-in prop-policy [:f/path :id]) (log/warn "NO :f/path PATH! FOR:  " prop-policy))
          prop-sid    (<? (dbproto/-subid db (get-in prop-policy [:f/path :id])))
          actions     (->> allow-rule :f/action util/sequential (map :id))
          allow-rule* (-> allow-rule
                          ;; remove :f/action as we end up keying the rule *per-action* in the final policy map, don't want confusion if this hangs around that it is used
                          (dissoc :f/action)
                          ;; associate our compiled function to the existing allow-rule map
                          (assoc :function fn-tuple))
          prop-ks     (map #(conj % prop-sid) default-key-seqs)]
      (for [policy-key-seq prop-ks
            action         actions]
        ;; for every policy-key-seqs (key sequence that can be used with (assoc-in m <ks-here> ...))
        ;; prepend key-sequence with the policy rule's action(s)
        (let [ks* (into [action] policy-key-seq)]
          ;; return two-tuple of [full-key-seq updated-allow-rule-map]
          [ks* allow-rule*])))))


(defn unrestricted-actions
  "A policy will be a 'root' (all access) policy if these 3 conditions apply:
   1) :f/targetNode value of :f/allNodes
   2) top-level (default) :f/allow rule that has no restrictions
   3) no additional :f/property restrictions for the respective :f/action granted

   Where a root policy exists for a specific action, returns the key sequences for each of the
   actions., e.g. [ [[:f/view :root?], true], [[:f/modify :root?], true] ] which will end up in
   the policy map like:
   {:f/view   {:root? true}
    :f/modify {:root? true}}

   At this stage, these root policies exist at the top-level :f/allow rule, but if we
   discover there is a more restrictive policy for a :f/property, they will be removed (due to
   condition #3 above not being met)."
  [node-policy]
  (let [root-actions (reduce
                       (fn [actions next-policy]
                         (if (restricted-allow-rule? next-policy)
                           ;; allow policy is restricted, therefore not 'root'
                           actions
                           (into actions (map :id (:f/action next-policy)))))
                       #{} (:f/allow node-policy))]
    (not-empty root-actions)))


(defn non-allNodes
  "Removes :f/allNodes from list of nodes.
  Returns nil if no other nodes exist."
  [nodes]
  (not-empty
    (remove #(= :f/allNodes %) nodes)))


(defn remove-root-default-allow
  "Removes :f/allow top-level policies that have already been granted
  root access as they no longer need to be considered in evaulation."
  [node-policy root-actions]
  (let [default-allow          (get node-policy :f/allow)
        non-root-default-allow (reduce
                                 (fn [acc allow-policy]
                                   (let [actions          (:f/action allow-policy)
                                         ;; root-actions is a set, remove any actions that are already root, which is likely is all of them
                                         non-root-actions (remove #(root-actions (:id %)) actions)]
                                     ;; if any actions are left, leave the policy with the remaining actions - else remove policy entirely
                                     (if (empty? non-root-actions)
                                       acc
                                       (conj acc (assoc allow-policy :f/action non-root-actions)))))
                                 [] default-allow)]
    ;; if there are no non-root default allow policies, remove :f/allow
    ;; else, replace with only non-root default allow policies
    (if (empty non-root-default-allow)
      (dissoc node-policy :f/allow)
      (assoc node-policy :f/allow non-root-default-allow))))

;; TODO - exceptions in here won't be caught!
(defn default-allow-restrictions
  "Takes default allow policy (:f/allow full-policy) and returns two-tuples of [key-seq policy-fn]
  where key-seq is the key sequence (as used by assoc-in) of where the policy function should
  land in the final policy map."
  [db base-key-seq default-allow]
  (go-try
    (let [default-allow-keys (map #(conj % :default) base-key-seq)]
      (->> default-allow                                    ;; calling function assumed to force into sequential if wasn't already
           (map #(compile-allow-rule db default-allow-keys %))
           async/merge
           (async/reduce
             (fn [acc result]
               (into acc result)) [])
           <?))))

;; TODO - exceptions in here won't be caught!
(defn property-restrictions
  "Takes property policies (:f/property full-policy) and returns two-tuples of [key-seq policy-fn]
  where key-seq is the key sequence (as used by assoc-in) of where the policy function should
  land in the final policy map."
  [db base-key-seq prop-policies]
  (go-try
    (->> prop-policies
         (map #(compile-prop-policy db base-key-seq %))
         async/merge
         (async/reduce
           (fn [acc result]
             (into acc result)) [])
         <?)))


(defn compile-node-policy
  "Compiles a node rule (where :f/targetNode is used).

  :f/targetNode that uses the special keyword :f/allNodes means that the target is for
  everything in the database. This is typically how a global 'root' policy role is
  established.

  One way to establish 'root, except x' policy would be to grant default allow policy for :f/allNodes
  but then limit access to a specific property using :f/property restriction for that same :f/action.

  Here we calculate all 'root' actions (if any), but then remove them if we later find a :f/property
  restriction for that same action."
  [db policy nodes]
  (go-try
    (let [all-nodes?               (some #(= :f/allNodes %) nodes)
          nodes*                   (when all-nodes?
                                     (non-allNodes nodes)
                                     nodes)
          root-actions             (when all-nodes?
                                     (unrestricted-actions policy))
          non-root-policy          (if root-actions         ;; if there was a root policy defined, remove default allows that contain it
                                     (remove-root-default-allow policy root-actions)
                                     policy)
          node-sids                (when nodes*             ;; if this policy was only to define root, this will be empty - so no need to create async chans
                                     (<? (subids db nodes*)))
          default-key-seqs         (map (fn [node-sid] [:node node-sid]) node-sids)
          ;; the default restrictions will exist only for non-root policies defined for :f/allow
          default-restrictions     (when-let [default-allow (some-> (:f/allow non-root-policy) util/sequential)]
                                     (<? (default-allow-restrictions db default-key-seqs default-allow)))
          ;; property restrictions can "cancel" out root policy - meaning policy was allowed to perform action on everything *except* these specified properties (which is non-root)
          property-restrictions    (when-let [prop-policies (some-> (:f/property policy) util/sequential)]
                                     (<? (property-restrictions db default-key-seqs prop-policies)))

          prop-restriction-actions (into #{} (map ffirst property-restrictions)) ;; "action" is the first item in the key sequence
          ;; need to remove any root actions that have property restrictions
          root-actions*            (remove prop-restriction-actions root-actions)
          root-policies            (map (fn [root-action] [[root-action :root?] true]) root-actions*)]

      (cond-> []
              root-policies (into root-policies)
              default-restrictions (into default-restrictions)
              property-restrictions (into property-restrictions)))))

(defn compile-class-policy
  "Compiles a class rule (where :f/targetClass is used)"
  [db policy classes]
  (go-try
    (let [class-sids            (<? (subids db classes))
          default-key-seqs      (map #(vector :class %) class-sids)
          default-restrictions  (when-let [default-allow (some-> (:f/allow policy) util/sequential)]
                                  (<? (default-allow-restrictions db default-key-seqs default-allow)))
          property-restrictions (when-let [prop-policies (some-> (:f/property policy) util/sequential)]
                                  (<? (property-restrictions db default-key-seqs prop-policies)))]
      (concat default-restrictions property-restrictions))))


(defn compile-policy
  [db policy]
  (go-try
    (let [classes (some->> policy :f/targetClass util/sequential (mapv :id))
          nodes   (some->> policy :f/targetNode util/sequential (mapv :id))]
      (cond-> []
              classes (into (<? (compile-class-policy db policy classes)))
              nodes (into (<? (compile-node-policy db policy nodes)))))))


(defn compile-policies
  "Compiles rules into a fn that returns truthy if, when given a flake, is allowed."
  [db policies]
  ;; TODO - if multiple rules target the same class, we need to 'or' the rules together.
  (->> policies
       (map #(compile-policy db %))
       async/merge
       (async/reduce (fn [acc compiled-policy]
                       (into acc compiled-policy)) [])))


(defn policy-map
  "perm-action is a set of the action(s) being filtered for."
  [db identity role credential]
  (async/go
    (try*
      (let [ident-sid         (<? (dbproto/-subid db identity))
            role-sids         (if (sequential? role)
                                (->> (<? (subids db role))
                                     (into #{}))
                                #{(<? (dbproto/-subid db role))})
            policies          {:ident ident-sid
                               :roles role-sids
                               :cache (atom {})}
            ;; TODO - query for all rules is very cacheable - but cache must be cleared when any new tx updates a rule
            ;; TODO - (easier said than done, as they are nested nodes whose top node is the only one required to have a specific class type)
            role-policies     (<? (policies-for-roles db policies))

            compiled-policies (->> (<? (compile-policies db role-policies))
                                   (reduce (fn [acc [ks m]]
                                             (assoc-in acc ks m))
                                           policies))
            root-access?      (= compiled-policies
                                 {:f/view {:node {:root? true}}})]
        (cond-> compiled-policies
                root-access? (assoc-in [:f/view :root?] true)))
      (catch* e
              (if (= :db/invalid-query (:error (ex-data e)))
                (throw (ex-info (str "There are no Fluree rules in the db, a policy-driven database cannot be retrieved. "
                                     "If you have created rules, make sure they are of @type f:Rule.")
                                {:status 400
                                 :error  :db/invalid-policy}))
                (throw e))))))


(defn wrap-policy
  "Given a db object, wraps specified policy permissions"
  [db identity role credential]
  ;; TODO - not yet paying attention to verifiable credentials that are present
  (go-try
    (assoc db :policy
              (<? (policy-map db identity role credential)))))
