(ns fluree.db.permissions
  (:require [fluree.db.dbproto :as dbproto]
            [fluree.db.dbfunctions.core :as dbfunctions]
            [#?(:cljs cljs.cache :clj clojure.core.cache) :as cache]
            [fluree.db.util.async :refer [<? go-try]]
            [fluree.db.util.log :as log :include-macros true]))

#?(:clj (set! *warn-on-reflection* true))

;; permissions are placed on a collection or predicate
;;
;; Fns resolve to true or false


;; cache for function generation
(def rule-fn-cache (atom #?(:clj  (cache/fifo-cache-factory {} :threshold 300)
                            :cljs (cache/lru-cache-factory {} :threshold 300))))

(defn parse-fn
  ([db fn-str]
   (parse-fn db fn-str nil))
  ([db fn-str params]
   (go-try
     (or (get @rule-fn-cache fn-str)
         (cond
           (or (true? fn-str) (= "true" fn-str))
           true

           (or (false? fn-str) (= "false" fn-str))
           false

           (re-matches #"^\(.+\)$" fn-str)
           (let [f-meta (<? (dbfunctions/parse-and-wrap-fn db fn-str "functionDec" params))]
             (swap! rule-fn-cache assoc fn-str f-meta)
             f-meta)

           :else
           (throw (ex-info (str "Invalid rule function provided: " fn-str)
                           {:status 400
                            :error  :db/invalid-fn})))))))

;; TODO - p-queries should be structured to execute all queries simultaneously
(defn parse-rules
  "Determine which collections, predicates, and operations this rule applies to. Then fetch all function refs
  from _rule/fns, and apply to all relevant "
  [db rule]
  (go-try
    (let [ops              (get rule "_rule/ops")
          collection       (get rule "_rule/collection")
          predicates       (get rule "_rule/predicates")
          fn-refs          (get rule "_rule/fns")
          default?         (get rule "_rule/collectionDefault")
          [params fn-strs] (loop [[ref & r] fn-refs
                                  params  []
                                  fn-strs []]
                             (if-not ref
                               [params fn-strs]
                               (let [params*  (conj params (get ref "_fn/params"))
                                     fn-strs* (conj fn-strs (get ref "_fn/code"))]
                                 (recur r params* fn-strs*))))
          params*          (remove empty? params)
          _                (if (empty? params*)
                             nil
                             (throw (ex-info (str "You can only use functions with additional parameters in transactions functions. ")
                                             {:status 400
                                              :error  :db/invalid-tx})))
          fn-str           (dbfunctions/combine-fns fn-strs)
          _                (when-not (and ops collection fn-str (or (not-empty predicates) default?))
                             (throw (ex-info (str "Incomplete rule, does not contain all required predicates for rule id:" (pr-str (get rule "_id")))
                                             {:status 400
                                              :error  :db/invalid-rule})))
          fun              (<? (parse-fn db fn-str nil))
          wild-collection? (or (nil? collection) (= "*" collection))
          wild-predicate?  (or (empty? predicates) (some #(= "*" %) predicates))

          partition        (when-not wild-collection?
                             (dbproto/-c-prop db :partition collection))
          predicate-ids    (when-not wild-predicate?
                             (map #(dbproto/-p-prop db :id %) predicates))]
      (cond
        ;; root! - with root, no other rules matter
        (and wild-collection? wild-predicate? (not default?) (true? fun))
        [[[:root?] true]]

        ;; global default, if predicate does exist, we ignore
        (and default? wild-collection?)
        [[[:collection :default] fun]]

        ;; predicate predicate(s), all collections but predicate specified
        (and wild-collection? (not wild-predicate?))
        (mapv (fn [pred-id] [[:predicate pred-id] fun]) predicate-ids)

        ;; A collection predicate, a collection default for this collection will no longer make any sense
        ;; first check collection + predicate rule(s) and if none are true, check this.
        (and (not wild-collection?) wild-predicate?)
        (if default?
          [[[:collection partition :default] fun]]
          [[[:collection partition :all] fun]])

        ;; collection + predicate predicate
        :else
        (mapv (fn [pred-id] [[:collection partition pred-id] fun]) predicate-ids)))))


(defn rules-from-role
  "Given a role, returns rules associated with it. Optionally can filter by
  a specific '_rule/ops' type as a keyword, i.e. :all, :query, :transact, :token."
  ([db role]
   (rules-from-role db role :all))
  ([db role filter-op-type]
   (go-try
     ;; TODO - rewrite -subject query
     (let [rules-res (<? (dbproto/-query db {:selectOne [{"_role/rules" ["*", {"_rule/fns" ["_fn/code" "_fn/params"]}]}]
                                             :from      role}))
           _         (when-not rules-res (throw (ex-info (str "Invalid role ident, doesn't exist: " (pr-str role))
                                                         {:status 400
                                                          :error  :db/invalid-role})))
           rules     (get rules-res "_role/rules")
           op-filter (when-not (= :all filter-op-type)
                       (set ["all" (name filter-op-type)]))]
       (if op-filter
         (filter #(->> (get % "_rule/ops")
                       (some (fn [op] (op-filter op))))
                 rules)
         rules)))))

;; TODO - clear cache on close
(def role-permission-cache (atom (cache/lru-cache-factory {} :threshold 500)))

;; TODO - can do parallelism below
(defn role-permissions
  "Given a role identity (_id), returns a permission map for the given permission type.
  Permission types supported are either :query or :transact."
  [{:keys [schema] :as db} role-ident permission-type]
  (go-try
    (or (when-not (:tt-id db)
          ;; schema's :t value is updated every time there is a new schema/fn/role change
          (get @role-permission-cache [(:t schema) (:network db) (:ledger-id db) role-ident permission-type]))
        (let [_              (when-not (#{:query :transact :token} permission-type)
                               (throw (ex-info (str "Invalid permission op type:" (pr-str permission-type))
                                               {:status 400
                                                :error  :db/invalid-role})))
              filtered-rules (<? (rules-from-role db role-ident permission-type))
              parsed-ruleset (loop [[rule & r] filtered-rules
                                    acc []]
                               (if-not rule
                                 acc
                                 (let [parsed-rules (<? (parse-rules db rule))]
                                   (recur r (into acc parsed-rules)))))]
          (swap! role-permission-cache assoc [(:t schema) (:network db) (:ledger-id db) role-ident permission-type] parsed-ruleset)
          parsed-ruleset))))


(defn permission-map
  [db roles permission-type]
  (go-try
    (let [all-parsed-rules (loop [[n & r] roles
                                  acc []]
                             (if-not n
                               acc
                               (->> (<? (role-permissions db n permission-type))
                                    (into acc)
                                    (recur r))))]
      (reduce
        (fn [acc [path function]]
          (update-in acc path #(cond
                                 ;; nothing exists there yet, so just place predicate
                                 (nil? %)
                                 (if (boolean? function)
                                   function
                                   [function])

                                 ;; any true boolean takes the prize
                                 (or (true? %) (true? function))
                                 true

                                 ;; both false, so it is false
                                 (and (false? %) (false? function))
                                 false

                                 ;; at least one must be a rule function
                                 (vector? %)
                                 (if (false? function)
                                   %
                                   (conj % function))

                                 (nil? %)
                                 [function])))
        {:root? false}
        all-parsed-rules))))
