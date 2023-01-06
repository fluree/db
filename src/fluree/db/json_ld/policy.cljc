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

(def special-meaning-properties
  #{:f/$identity})

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
  (keep
    (fn [rule]
      (let [class-rules    (->> (get rule :f/allow)
                                util/sequential
                                (filter #(roles (get-in % [:f/targetRole :_id])))
                                not-empty)
            property-rules (when-let [property-rules (get rule :f/property)]
                             ;; each explicit property can have multiple :f/allow targeting different roles
                             ;; We only want :f/allow that target provided roles, and only want properties
                             ;; returned tht contain at least one relevant :f/allow
                             (filter
                               (fn [property-rule]
                                 (let [roles-rules (->> (get property-rule :f/allow)
                                                        util/sequential
                                                        (filter #(roles (get-in % [:f/targetRole :_id])))
                                                        not-empty)]
                                   (when roles-rules
                                     (assoc property-rule :f/allow roles-rules))))
                               (util/sequential property-rules)))]
        (when (or class-rules property-rules)
          (cond-> rule
                  class-rules (assoc :f/allow class-rules)
                  property-rules (assoc :f/property property-rules)))
        ;; if class rules doesn't exist for roles, check if any property rules exist
        ))
    all-rules))

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


(defmulti parse-rule (fn [_ rule]
                       (cond
                         (contains? rule :f/equals) :f/equals
                         (contains? rule :f/contains) :f/contains
                         :else ::unrestricted-rule)))

(defmethod parse-rule :f/equals
  [db rule]
  (go-try
    (let [resolved-path (<? (property-path db :f/equals rule))]
      (validate/generate-equals-fn rule resolved-path))))

(defmethod parse-rule :f/contains
  [db rule]
  ;; TODO
  (go-try
    (throw (ex-info ":f/contains not yet implemented!" {}))))

(defmethod parse-rule ::unrestricted-rule
  [db rule]
  ;; there are no conditions on the rule, which means explicitly allow
  (go-try
    [false (constantly true)]))


(defn compile-property-rules
  "Returns a map with property ids as keys with two-tuple value of async? + policy function.

  If function is async downstream the value will need to be retrieved downstream."
  [db rule]
  (go-try
    (let [property-rules (util/sequential rule)]
      (loop [[prop-rule & r] property-rules
             acc {}]
        (log/warn "Compiling property rule: " prop-rule)
        (if prop-rule
          (let [allow-spec (:f/allow prop-rule)
                fn-tuple   (if (sequential? allow-spec)
                             (do
                               ;; TODO - Multiple conditions existing for a single role/property is probably a valid use case but uncommon.
                               ;; TODO --- they should be treated as an -OR- condition and could be wrapped into a single fn.
                               ;; TODO --- the wrapping fn would have to look for async fns and use <? takes (and itself be async)
                               (log/warn (str "Multiple role rules for same property is not currently allowed. Using only first "
                                              "allow specification in rule: " prop-rule "."))
                               (<? (parse-rule db (first allow-spec))))
                             (<? (parse-rule db allow-spec))) ;; returns two-tuple of [async? validation-fn]
                prop-sid   (<? (dbproto/-subid db (get-in prop-rule [:f/path :id])))]
            ;; TODO - if multiple rules target the same path we need to concatenate them and should use an 'or' condition
            (when (get acc prop-sid)
              (log/warn (str "Multiple policy rules in the same class target the same property: "
                             (get prop-rule :f/path) ". Only the last one encountered will be utilized.")))
            (recur r (assoc acc prop-sid fn-tuple)))
          acc)))))


(defn compile-class-rule
  "Compiles a class rule (where :f/targetClass is used)"
  [db action rule classes]
  (go-try
    (let [class-sids            (<? (subids db classes))
          allow-spec            (get rule :f/allow)
          default-restrictions  (if (sequential? allow-spec)
                                  (<? (parse-rule db (first allow-spec)))
                                  (<? (parse-rule db allow-spec)))
          property-restrictions (when-let [prop-rules (:f/property rule)]
                                  (<? (compile-property-rules db prop-rules)))
          all-restrictions      (assoc property-restrictions :default default-restrictions)]
      ;; for each class targeted by the rule, map to each compiled fn
      (reduce
        (fn [acc class-sid]
          (assoc acc class-sid all-restrictions))
        {} class-sids))))


(defn compile-node-rule
  "Compiles a node rule (where :f/targetNode is used)"
  [db action rule nodes]
  (go-try
    (let [node-sids            (<? (subids db nodes))
          allow-spec           (get rule :f/allow)
          default-restrictions (if (sequential? allow-spec)
                                 (<? (parse-rule db (first allow-spec)))
                                 (<? (parse-rule db allow-spec)))]
      (when (:f/property rule)
        (log/warn "Currently, property based restrictions are not yet enforced. Found for nodes: " nodes))
      ;; for each class targeted by the rule, map to each compiled fn
      (if (and (= [:f/allNodes] nodes)
               (nil? (->> node-sids first (get default-restrictions))))
        {:root? true}
        (reduce
          (fn [acc node-sid]
            (assoc acc node-sid default-restrictions))
          {} node-sids)))))


(defn compile-rule
  [db action rule]
  (go-try
    (let [classes (some->> rule :f/targetClass util/sequential (mapv :id))
          nodes   (some->> rule :f/targetNode util/sequential (mapv :id))]
      (cond
        classes {:class (<? (compile-class-rule db action rule classes))}
        nodes {:node (<? (compile-node-rule db action rule nodes))}))))


(defn compile-rules
  "Compiles rules into a fn that returns truthy if, when given a flake, is allowed."
  [db action rules]
  ;; TODO - if multiple rules target the same class, we need to 'or' the rules together.
  (->> rules
       (map #(compile-rule db action %))
       async/merge
       (async/into [])))


(defn find-rules
  "Returns all the rules for the provided roles as compiled functions"
  [db {:keys [roles]}]
  ;; TODO - no caching is being done here yet, need to implement for at least all-rules lookup
  (go-try
    (let [all-rules (<? (all-rules db))]
      (rules-for-roles roles all-rules))))

(defn permission-map
  "perm-action is a set of the action(s) being filtered for."
  [db action identity role credential]
  (async/go
    (try*
      (let [ident-sid      (<? (dbproto/-subid db identity))
            role-sids      (if (sequential? role)
                             (->> (<? (subids db role))
                                  (into #{}))
                             #{(<? (dbproto/-subid db role))})
            permissions    {:ident ident-sid
                            :roles role-sids
                            :cache (atom {})
                            :root? nil}
            rules          (<? (find-rules db permissions))
            _              (log/warn "rules full: " rules)
            compiled-rules (->> (<? (compile-rules db action rules))
                                (apply merge))
            root-rule?     (= compiled-rules
                              {:node {:root? true}})]
        (cond-> (assoc permissions :view compiled-rules)
                root-rule? (assoc :root? true)))
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
    (assoc db :permissions
              (<? (permission-map db #{:f/view} identity role credential)))))