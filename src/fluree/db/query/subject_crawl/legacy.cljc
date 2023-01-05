(ns fluree.db.query.subject-crawl.legacy
  (:require [fluree.db.util.core :as util #?(:clj :refer :cljs :refer-macros) [try* catch*]]
            [fluree.db.dbproto :as dbproto]
            [clojure.string :as str]
            [fluree.db.spec :as spec]
            [fluree.db.util.async :refer [<? go-try]]
            [fluree.db.query.schema :as schema]
            [fluree.db.util.log :as log :include-macros true]))

;; handling for legacy Fluree 'basic queries'

#?(:clj (set! *warn-on-reflection* true))

(defn where-clause-valid?
  "Checks to see if the where clause has ' = ', ' > ', ' < ', ' <= ', or ' >= ', and returns true if yes"
  [where-clause]
  (and (string? where-clause)
       (re-find #"^.* (=|>|<|>=|<=|not=) .*$" where-clause)))

(defn parse-where-spec
  "Returns a where clause spec based on a string where caluse.
  The spec can be applied to a specific db to coerce predicate names
  to predicate ids, validate types, and ultimately generate a filtering function."
  ([where]
   (parse-where-spec where nil))
  ([where default-collection]
   (when-not (where-clause-valid? where) (throw (ex-info (str "Invalid where clause: " where)
                                                         {:status 400 :error :db/invalid-query})))
   (let [and?       (re-matches #".+ AND .+" where)
         or?        (re-matches #".+ OR .+" where)
         _          (when (and and? or?)
                      (throw (ex-info (str "Where clause can have either AND or OR operations, but not both currently: " where)
                                      {:status 400 :error :db/invalid-query})))
         where-type (cond
                      and?
                      :and

                      or?
                      :or

                      :else
                      :and)
         args       (case where-type
                      :and (str/split where #" AND ")
                      :or (str/split where #" OR "))
         statements (reduce
                      (fn [acc arg]
                        (let [arg       (str/trim arg)
                              [_ ^String pred-name ^String op ^String match] (re-find #"^([^\s=><].+)[\s]+:?(=|>|<|>=|<=|not=)[\s]+:?(.+)$" arg)
                              pred-name (cond
                                          (schema/reverse-ref? pred-name false)
                                          (throw (ex-info (str "Reverse references cannot be used in a where clause. Provided: " pred-name)
                                                          {:status 400 :error :db/invalid-query}))

                                          (str/includes? pred-name "/")
                                          pred-name

                                          default-collection
                                          (str default-collection "/" pred-name)

                                          :else
                                          (throw (ex-info (str "Only full namespaced predicate
                                         names can be used in a where clause. This can be
                                         provided in a from clause. Provided: " pred-name)
                                                          {:status 400 :error :db/invalid-query})))
                              match*    (if-let [match-str (or (re-find #"'(.*)'$" match) (re-find #"\"(.*)\"$" match))]
                                          (second match-str)
                                          ;; must be numeric
                                          ;; TODO - we should look up predicate type and do conversion according to it
                                          (try*
                                            (cond
                                              (= "true" match)
                                              true

                                              (= "false" match)
                                              false

                                              (str/includes? match ".")
                                              #?(:clj  (Double/parseDouble match)
                                                 :cljs (js/parseFloat match))

                                              :else
                                              #?(:clj  (Long/parseLong match)
                                                 :cljs (js/parseInt match)))
                                            (catch* _
                                                    (throw (ex-info (str "Invalid where clause in argument: " arg)
                                                                    {:status 400
                                                                     :error  :db/invalid-query})))))]
                          (conj acc [pred-name op match*])))
                      [] args)]
     [where-type statements])))

(defn into-where
  [where]
  (let [[where-type where-statements] (parse-where-spec where)
        vars     (volatile! 1)
        next-var (fn [] (str "?__" (vswap! vars inc)))
        where*   (mapv
                   (fn [[pred op val]]
                     (if (= "=" op)
                       ["?s" pred val]
                       ["?s" pred (str "#(" op " " (next-var) " " val ")")]))
                   where-statements)]
    (cond
      (= :and where-type) where*
      (= :or where-type) [{:optional where*}])))

(defn basic-to-analytical-transpiler
  [db query-map]
  (let [{:keys [select selectOne selectDistinct where from vars]} query-map
        selectKey  (cond select :select
                         selectOne :selectOne
                         selectDistinct :selectDistinct)
        select-smt (or select selectOne selectDistinct)
        multi-subj (when (sequential? from)
                     from)
        vars*      (if multi-subj
                     (assoc vars "?__subj" from)
                     vars)
        where*     (cond
                     ;; for multi-subject, we'll pass the subjects in :vars {"?__subj" [subj1, subj2, ...]}
                     multi-subj
                     [["?s" "_id" "?__subj"]]

                     (string? where)
                     (throw (ex-info "String where queries not allowed for json-ld databases."
                                     {:status 400 :error :db/invalid-query}))

                     ; Single subject - subject _id
                     (number? from)
                     [["?s" "_id" from]]

                     (or (string? from) (keyword? from))
                     [["?s" "@id" from]]

                     ;; Legacy predicate-based query
                     (and (string? from) (str/includes? from "/"))
                     [["?s" from nil]]

                     ;; Legacy collection-based query
                     (string? from)
                     [["?s" "rdf:type" from]]

                     ; Single subject - two-tuple
                     (util/pred-ident? from)
                     [["?s" (first from) (second from)]]

                     :else
                     (ex-info (str "Invalid 'from' in query:" (pr-str query-map))
                              {:status 400 :error :db/invalid-query}))]

    (-> query-map
        (dissoc :from)
        (assoc selectKey {"?s" select-smt}
               :where    where*)
        (cond-> vars* (assoc :vars vars*)))))
