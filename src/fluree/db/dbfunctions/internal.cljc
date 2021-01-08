(ns fluree.db.dbfunctions.internal
  (:refer-clojure :exclude [max min get inc dec + - * / == quot mod rem contains? get-in < <= > >=
                            boolean re-find and or count str nth rand nil? empty? hash-set not subs not=])
  (:require [clojure.tools.reader.edn :as edn]
            [fluree.db.query.fql :as fql]
            [fluree.db.util.core :as util :refer [try* catch*]]
            #?(:clj  [clojure.core.async :as async]
               :cljs [cljs.core.async :as async])
            [fluree.db.util.log :as log]
            [fluree.db.util.async :refer [go-try <?]]
            [fluree.db.dbproto :as dbproto]
            [clojure.string :as str]))

(defn- parse-select-map
  [param-str]
  (let [parsed-param (if (string? param-str) (edn/read-string param-str) param-str)]
    (cond
      (map? parsed-param)
      (let [key     (first (keys parsed-param))
            key'    (if (string? key) key
                                      (if (clojure.core/nil? (namespace key))
                                        (name key)
                                        (clojure.core/str (namespace key) "/" (name key))))
            value   (first (vals parsed-param))
            value'  (parse-select-map value)
            value'' (if (coll? value')
                      (into [] value')
                      value')]
        (assoc {} key' value''))

      (string? parsed-param)
      parsed-param

      (clojure.core/or (symbol? parsed-param) (var? parsed-param))
      (if (clojure.core/nil? (namespace parsed-param))
        (name parsed-param)
        (clojure.core/str (namespace parsed-param) "/" (name parsed-param)))

      (vector? parsed-param)
      (mapv parse-select-map parsed-param)

      :else
      (throw (ex-info (clojure.core/str "The query path is not properly formatted: " parsed-param)
                      {:status 400
                       :error  :db/invalid-fn})))))

(defn- function-error
  [e function-name & args]
  (log/error e "Function Error: " function-name "args: " (pr-str args))
  (throw (ex-info (clojure.core/str "Error in database function: " function-name ": " (if (clojure.core/nil? (.getMessage e)) (.getClass e) (.getMessage e))
                                    ". Provided: " (if (coll? args) (clojure.string/join " " args) args))
                  {:status 400
                   :error  :db/invalid-fn})))

(defn boolean
  "Coerce to boolean. Everything except `false' and `nil' is true in boolean context."
  [x]
  (try* (clojure.core/boolean x)
        (catch* e (function-error e "boolean" x))))

(defn nil?
  [arg]
  (try* (clojure.core/nil? arg)
        (catch* e (function-error e "nil?" arg))))

(defn not
  [arg]
  (try* (clojure.core/not arg)
        (catch* e (function-error e "not" arg))))

(defn empty?
  [arg]
  (try* (clojure.core/or (clojure.core/empty? arg) (= #{nil} arg))
        (catch* e (function-error e "empty?" arg))))

(defn if-else
  "Like clojure.core/if"
  [test true-res false-res]
  (try* (cond test
              true-res

              :else
              false-res)
        (catch* e (function-error e "if-else" test true-res false-res))))

(defn and
  "Returns true if all true"
  [& args]
  (try*
    (let [coerced-coll (map boolean args)]
      (if (nil? args)
        false
        (boolean (every? true? coerced-coll))))
    (catch* e (function-error e "and" args))))

(defn or
  "Returns true if any true"
  [& args]
  (try*
    (let [coerced-coll (map boolean args)]
      (if (nil? args)
        false
        (boolean (some true? coerced-coll))))
    (catch* e (function-error e "or" args))))

(defn count
  "Returns the number of items in the collection. (count nil) returns 0.  Also works on strings, arrays, and Java Collections and Maps"
  [coll]
  (try* (clojure.core/count coll)
        (catch* e (function-error e "count" coll))))

(defn str
  "Like clojure.core/str"
  [& args]
  (try* (apply clojure.core/str args)
        (catch* e (function-error e "str" args))))

(defn subs
  "Like clojure.core/subs"
  [& args]
  (try* (apply clojure.core/subs args)
        (catch* e (function-error e "subs" args))))

(defn lower-case
  "Like clojure.core/lower-case"
  [str]
  (try* (clojure.string/lower-case str)
        (catch* e (function-error e "lower-case" str))))

(defn upper-case
  "Like clojure.core/upper-case"
  [str]
  (try* (clojure.string/upper-case str)
        (catch* e (function-error e "upper-case" str))))

(defn max
  "Like clojure.core/max, but applies max on a sequence"
  [& args]
  (try* (apply clojure.core/max (remove nil? args))
        (catch* e (function-error e "max" args))))

(defn min
  "Like clojure.core/min, but applies min on a sequence"
  [& args]
  (try* (apply clojure.core/min (remove nil? args))
        (catch* e (function-error e "min" args))))

(defn >
  "Like clojure.core/>, but applies > on a sequence"
  [& args]
  (try* (apply clojure.core/> args)
        (catch* e (function-error e ">" args))))

(defn >=
  "Like clojure.core/>=, but applies > on a sequence"
  [& args]
  (try* (apply clojure.core/>= args)
        (catch* e (function-error e ">=" args))))


(defn <
  "Like clojure.core/>, but applies < on a sequence"
  [& args]
  (try* (apply clojure.core/< args)
        (catch* e (function-error e "<" args))))

(defn <=
  "Like clojure.core/>, but applies < on a sequence"
  [& args]
  (try* (apply clojure.core/<= args)
        (catch* e (function-error e "<=" args))))

(defn not=
  [& args]
  (try* (apply clojure.core/not= args)
        (catch* e (function-error e "not=" args))))

(defn query
  "Executes a database query, but returns the :results directly."
  ([db query-map]
   (go-try
     (let [fuel     (volatile! 0)
           max-fuel 100000
           opts     (assoc (:opts query-map) :fuel fuel
                                             :max-fuel max-fuel)
           result   (async/<! (fql/query db (assoc query-map :opts opts)))]
       (if (util/exception? result)
         (function-error result "query" query-map)
         [result @fuel]))))
  ([db select from where block limit]
   (async/go
     (let [parsed-select (if (string? select)
                           (parse-select-map select)
                           select)
           fuel          (volatile! 0)
           max-fuel      100000
           query         (util/without-nils
                           {:select parsed-select
                            :from   from
                            :where  where
                            :block  block
                            :limit  limit
                            :opts   {:fuel fuel :max-fuel max-fuel}})
           query'        (if (and (:where query)
                                  (= "[" (str (first (:where query))))
                                  (= "]" (str (last (:where query)))))
                           (let [where (#?(:clj read-string :cljs cljs.reader/read-string) (:where query))]
                             (assoc query :where where)) query)

           result        (async/<! (fql/query db query'))]
       (if (util/exception? result)
         result
         [result @fuel])))))

(def pred-reverse-ref-re #"(?:([^/]+)/)_([^/]+)")

(defn reverse-ref?
  "Reverse refs must be strings that include a '/_' in them, which characters before and after."
  [predicate-name]
  (if (string? predicate-name)
    (boolean (re-matches pred-reverse-ref-re predicate-name))
    (throw (ex-info (str "Bad predicate name, should be string: " (pr-str predicate-name))
                    {:status 400
                     :error  :db/invalid-predicate}))))

(defn unreverse-var
  "A reverse-reference predicate gets transformed to the actual predicate name."
  [pred]
  (str/replace pred "/_" "/"))

(defn build-where-single-path
  [startSubject var endSubject]
  (let [start-clause  [startSubject var "?var1"]
        bridge-clause ["?var2" var "?var1"]
        end-clause    ["?var2" var endSubject]]
    [start-clause bridge-clause end-clause]))

(defn- build-where-clause
  [startSubject path endSubject]
  (loop [[var & r] path
         n             1
         where-clauses []]
    (cond
      ;; A relationship with a single path is a special case.
      ;; We need to use a bridge clause, i.e.:
      ;;   [startSubject "_user/auth" "?varAuth"]
      ;;   ["?varUser" "_user/auth" "?varAuth"]
      ;;   ["?varUser" "_user/auth" endSubject]
      (and (empty? where-clauses) (nil? r))
      (let [reverse? (reverse-ref? var)]
        (if reverse? (build-where-single-path endSubject (unreverse-var var) startSubject)
                     (build-where-single-path startSubject var endSubject)))

      (empty? where-clauses)
      (let [reverse?     (reverse-ref? var)
            next-clauses (if reverse?
                           [[(str "?var" n) (unreverse-var var) startSubject]]
                           [[startSubject var (str "?var" n)]])]
        (recur r n next-clauses))

      r (let [next-n      (clojure.core/inc n)
              reverse?    (reverse-ref? var)
              next-clause (if reverse?
                            [(str "?var" next-n) (unreverse-var var) (str "?var" n)]
                            [(str "?var" n) var (str "?var" next-n)])]
          (recur r next-n (conj where-clauses next-clause)))

      ;; last clause
      :else
      (let [reverse?    (reverse-ref? var)
            last-clause (if reverse?
                          [endSubject (unreverse-var var) (str "?var" n)]
                          [(str "?var" n) var endSubject])]
        (->> last-clause
             (conj where-clauses))))))

(defn relationship?
  [db startSubject path endSubject]
  (go-try (let [path          (if (vector? path) path [path])
                where-clauses (build-where-clause startSubject path endSubject)
                rel-q         {:select "?var1" :where where-clauses}]
            (try*
              (<? (query db rel-q))
              (catch* e
                      (function-error e "relationship?" startSubject path endSubject))))))

(defn inc
  "Increments by 1. nil is treated as zero."
  [n]
  (try*
    (if (nil? n)
      1
      (clojure.core/inc n))
    (catch* e (function-error e "inc" n))))

(defn dec
  "Decrements by 1. nil is treated as zero."
  [n]
  (try*
    (if (nil? n)
      -1
      (clojure.core/dec n))
    (catch* e (function-error e "dec" n))))

(defn get
  [m k]
  (try*
    (clojure.core/or (clojure.core/get m k) (clojure.core/get m (keyword k)))
    (catch* e (function-error e "get" m k))))

(defn now
  "Returns current epoch milliseconds."
  []
  (try*
    (util/current-time-millis)
    (catch* e (function-error e "now"))))

(defn +
  "Returns sum of all arguments in a sequence."
  [& args]
  (try*
    (apply clojure.core/+ args)
    (catch* e (function-error e "+" args))))

(defn -
  "Returns difference of all the numbers in the sequence with the first number as the minuend."
  [& args]
  (try*
    (apply clojure.core/- args)
    (catch* e (function-error e "-" args))))

(defn *
  "Returns product of all the numbers in the sequence."
  [& args]
  (try*
    (if (clojure.core/or (nil? args) (empty? args))
      1
      (apply clojure.core/* args))
    (catch* e (function-error e "*" args))))

(defn /
  "If no denominators are supplied, returns 1/numerator, else returns numerator divided by all of the denominators. Takes a sequence"
  [& args]
  (try*
    (if (nil? args)
      (throw (ex-info (clojure.core/str "Function / takes at least one argument")
                      {:status 400
                       :error  :db/invalid-fn}))
      (apply clojure.core// args))
    (catch* e (function-error e "/" args))))

(defn quot
  "Quot[ient] of dividing numerator by denominator."
  [n d]
  (try*
    (clojure.core/quot n d)
    (catch* e (function-error e "quot" n d))))

(defn mod
  "Modulus of num and div. Truncates toward negative infinity."
  [n d]
  (try*
    (clojure.core/mod n d)
    (catch* e (function-error e "mod" n d))))

(defn rem
  "Remainder of dividing numerator by denominator."
  [n d]
  (try*
    (clojure.core/rem n d)
    (catch* e (function-error e "rem" n d))))

(defn ceil
  "Returns the ceiling of a number, as integer."
  [num]
  (try*
    (int (Math/ceil num))
    (catch* e (function-error e "ceil" num))))

(defn floor
  "Returns the floor of a number, as integer."
  [num]
  (try*
    (int (Math/floor num))
    (catch* e (function-error e "floor" num))))

(defn get-all
  "Follows an subject down the provided path and returns a set of all matching subjects."
  [start-subject path]
  (try*
    (loop [[pred & r] path
           subjects #{start-subject}]
      (let [next-subjects (reduce (fn [acc subject]
                                    (let [sub-subjects (if (vector? subject)
                                                         (mapv #(get % pred) subject)
                                                         (get subject pred))]
                                      (if
                                        (clojure.core/or (vector? sub-subjects) (set? sub-subjects))
                                        ;; multi-cardinality, combine
                                        (into acc sub-subjects)

                                        ;; single-cardinality - conj
                                        (conj acc sub-subjects))))
                                  #{} subjects)]
        (if (clojure.core/and r (not-empty next-subjects))
          (recur r next-subjects)
          (->> next-subjects (remove nil?) set))))
    (catch* e (function-error e "get-all" start-subject path))))

(defn get-in
  "Returns the value in a nested structure"
  [m ks]
  (try*
    (get-all m ks)
    (catch* e (function-error e "get-in" m ks))))

(defn contains?
  "Returns true if key is present."
  [coll key]
  (try*
    (clojure.core/contains? coll key)
    (catch* e (function-error e "contains?" coll key))))

(defn hash-set
  "Returns a hash-set of args."
  [& args]
  (try*
    (apply clojure.core/hash-set args)
    (catch* e (function-error e "hash-set" args))))

(defn nth
  "Returns true if key is present."
  [coll key]
  (try*
    (let [coll' (if (set? coll)
                  (into [] coll)
                  coll)]
      (clojure.core/nth coll' key))
    (catch* e (function-error e "nth" coll key))))

(defn ==
  "Return true if arguments in sequence equal each other."
  [& args]
  (try*
    (apply = args)
    (catch* e (function-error e "==" args))))

(defn re-find
  "Returns the next regex match, if any, of string to pattern, using java.util.regex.Matcher.find().  Uses re-groups to return the groups."
  [pattern string]
  (try*
    (clojure.core/re-find (re-pattern pattern) string)
    (catch* e (function-error e "re-find" pattern string))))

(defn ?pO
  [?ctx]
  (async/go
    (try*
      (if (string? (:sid ?ctx))                             ;; new entity -it will never be string
        [nil 0]
        (let [db        (:db ?ctx)
              prevT     (inc (:t db))
              db'       (assoc db :t prevT)
              prev-vals (<? (dbproto/-search db' [(:sid ?ctx) (:pid ?ctx)])) ;; could be multi-cardinality, only take first one (consider throwing?)
              fuel      (count prev-vals)
              pO        (some-> (first prev-vals)
                                (.-o))]
          ;predName (dbproto/-p-prop db :name (:pid ?ctx))
          ;pOQuery  {:select [predName]
          ;          :from   sid}
          ;[res2 fuel2] (if sid (<? (query db' pOQuery)) [nil nil])
          ;pO       (get res2 predName)

          [pO fuel]))
      (catch* e
              (try*
                (function-error e "?pO" "Context Object")
                (catch* e' e'))))))

(defn max-pred-val
  [db pred-name opts]
  (async/go
    (try*
      (let [[res fuel] (<? (query db {:select "?o" :where [[nil pred-name "?o"]] :opts opts}))]
        (if-not (empty? res) [(apply max res) fuel] [nil fuel]))
      (catch* e
              (try*
                (function-error e "max-pred-val" pred-name)
                (catch* e' e'))))))

(defn valid-email?
  [email]
  (try*
    (let [pattern #"[a-z0-9!#$%&'*+/=?^_`{|}~-]+(?:\.[a-z0-9!#$%&'*+/=?^_`{|}~-]+)*@(?:[a-z0-9](?:[a-z0-9-]*[a-z0-9])?\.)+[a-z0-9](?:[a-z0-9-]*[a-z0-9])?"]
      (clojure.core/boolean (clojure.core/and (string? email) (re-matches pattern email))))
    (catch* e (function-error e "valid-email?" email))))

(defn ?s
  "Retrieves all P-O Pairs for subject, potential additional params if specified."
  ([?ctx]
   (?s ?ctx nil))
  ([?ctx additional-params]
   (async/go
     (try*
       (if (clojure.core/and (:s ?ctx) (empty? additional-params))
         (:s ?ctx)
         (let [db     (:db ?ctx)
               sid    (clojure.core/or (:sid ?ctx)
                                       (get (:s ?ctx) :_id))
               select (if additional-params
                        (into [] (concat ["*"] (parse-select-map additional-params)))
                        ["*"])
               [res fuel] (<? (query db {:select select
                                         :from   sid
                                         :opts   {}}))]
           [res fuel]))
       (catch* e
               (try* (function-error e "?s" "Context Object" additional-params)
                     (catch* e' e')))))))

(defn ?p
  "Retrieves all P-O Pairs for predicate, potential additional params if specified"
  ([?ctx]
   (?p ?ctx nil))
  ([?ctx additional-params]
   (async/go
     (try*
       (let [db     (:db ?ctx)
             pid    (:pid ?ctx)
             select (if additional-params
                      (into [] (concat ["*"] (parse-select-map additional-params)))
                      ["*"])
             [res fuel] (<? (query db {:select select
                                       :from   pid
                                       :opts   {}}))]
         [res fuel])
       (catch* e (function-error e "?p" "Context Object" additional-params))))))

(defn ?user_id-from-auth
  [?ctx]
  (async/go
    (try*
      (let [query' {:select [{"_user/_auth" ["*"]}]
                    :from   (:auth_id ?ctx)
                    :opts   {}}
            [res fuel] (<? (query (:db ?ctx) query'))
            user   (first (get-in res ["_user/_auth" "_id"]))]
        [user fuel])
      (catch* e (function-error e "?user_id-from-auth" "Context Object")))))

(defn ?auth_id
  [?ctx]
  (go-try (let [auth (:auth_id ?ctx)]
            (<? (dbproto/-subid (:db ?ctx) auth)))))

(defn objT
  "Given an array of flakes, returns the sum of the objects of the true flakes"
  [flakes]
  (try*
    (let [trueF (filterv #(true? (.-op %)) flakes)
          objs  (map #(.-o %) trueF)
          sum   (reduce clojure.core/+ objs)]
      sum)
    (catch* e (function-error e "objT" flakes))))

(defn objF
  "Given an array of flakes, returns the sum of the objects of the false flakes"
  [flakes]
  (try*
    (let [falseF (filterv #(false? (.-op %)) flakes)
          objs   (map #(.-o %) falseF)
          sum    (reduce clojure.core/+ objs)]
      sum)
    (catch* e (function-error e "objF" flakes))))

(defn rand
  [instant max']
  (try*
    (let [base (.nextDouble (java.util.Random. instant))
          num  (int (Math/floor (* base max')))] num)
    (catch* e (function-error e "rand" instant max'))))

(defn cas
  "Returns new-val if existing-val is equal to compare-val, else throws exception"
  [?ctx compare-val new-val]
  (go-try
    (let [{:keys [sid pid db]} ?ctx
          p-name      (dbproto/-p-prop db :name pid)
          _           (when-not sid
                        (throw (ex-info (str "Unable to execute cas - subject id could be determined. Cas values: " compare-val new-val)
                                        {:status 400
                                         :error  :db/validation-error})))
          _           (when-not p-name
                        (throw (ex-info (str "Unable to execute cas - predicate could be determined. Cas values: " compare-val new-val)
                                        {:status 400
                                         :error  :db/validation-error})))
          _           (when (dbproto/-p-prop db :multi pid)
                        (throw (ex-info (str "Unable to execute cas on a multi-cardinality predicate: " p-name)
                                        {:status 400
                                         :error  :db/validation-error})))
          [res _] (<? (query db {:select "?current-val"
                                 :where  [[sid p-name "?current-val"]]
                                 :opts   {}}))
          current-val (first res)]
      (if (= current-val compare-val)
        new-val
        (throw (ex-info (clojure.core/str "The current value: " current-val " does not match the comparison value: " compare-val ".")
                        {:status 400
                         :error  :db/validation-error}))))))

