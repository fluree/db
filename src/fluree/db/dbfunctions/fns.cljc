(ns fluree.db.dbfunctions.fns
  (:refer-clojure :exclude [max min get inc dec + - * / quot mod == rem contains? get-in < <= > >=
                            boolean re-find and or count str nth rand nil? hash-set empty? not uuid subs not=])
  (:require [fluree.db.dbfunctions.internal :as fdb]
            [fluree.db.util.log :as log]
            [fluree.db.util.json :as json]
            [fluree.db.util.async :refer [channel? go-try <?]]
            [fluree.db.util.core :as util])
  #?(:cljs (:require-macros [fluree.db.dbfunctions.fns :refer [extract]])))


(defmacro extract
  "Resolves a value if a channel.
  Must be used inside of a go-block."
  [v]
  `(if (channel? ~v)
     (<? ~v)
     ~v))

(defn- coerce-args
  "Coerces args that may be core async channels into values.
  Returns exception instead of args if any exception occurs during resolution."
  [args]
  (go-try
    (loop [[arg & r] args
           acc []]
      (if-not arg
        acc
        (if (channel? arg)
          (recur r (conj acc (<? arg)))
          (recur r (conj acc arg)))))))

(defn stack
  "Returns the current stack."
  [?ctx]
  (-> @(:state ?ctx)
      :stack))

(defn- add-stack
  "Adds an entry to the current stack."
  [?ctx entry]
  (let [[res cost] entry]
    (do
      (log/debug "Smart function stack: " res)
      (swap! (:state ?ctx) (fn [s]
                             (assoc s :stack (conj (:stack s) entry)
                                      :credits (fdb/- (:credits s) cost)
                                      :spent (fdb/+ (:spent s) cost)))))))

(defn- raise
  "Throws an exception with the provided message."
  [?ctx msg]
  (throw (ex-info msg
                  {:status 400
                   :error  :db/invalid-fn
                   :stack  (stack ?ctx)})))

(defn nth
  {:doc      "Returns the nth item in a collection"
   :fdb/spec nil
   :fdb/cost "9 + count of objects in collection"}
  [?ctx coll key]
  (go-try
    (let [coll  (extract coll)
          coll  (if (set? coll) (vec coll) coll)
          key   (extract key)
          res   (fdb/nth coll key)
          cost  (clojure.core/+ 9 (clojure.core/count coll))
          entry [{:function "nth" :arguments [coll key] :result res} cost]]
      (add-stack ?ctx entry)
      res)))

(defn if-else
  {:doc      "Evaluates test."
   :fdb/spec nil
   :fdb/cost 10}
  [?ctx test t f]
  (go-try
    (let [test  (extract test)
          [t f] (if test [(extract t) f] [f (extract f)])
          res   (fdb/if-else test t f)
          entry [{:function "if-else" :arguments [test t f] :result res} 10]]
      (add-stack ?ctx entry)
      res)))

(defn nil?
  {:doc      "True if nil, else false."
   :fdb/spec nil
   :fdb/cost 10}
  [?ctx arg]
  (go-try
    (let [arg   (extract arg)
          res   (fdb/nil? arg)
          entry [{:function "nil?" :arguments [arg] :result res} 10]]
      (add-stack ?ctx entry)
      res)))

(defn not
  {:doc      "Takes a boolean, true returns false, false returns true."
   :fdb/spec nil
   :fdb/cost 10}
  [?ctx arg]
  (go-try (let [arg   (extract arg)
                res   (fdb/not arg)
                entry [{:function "not?" :arguments [arg] :result res} 10]]
            (add-stack ?ctx entry)
            res)))

(defn empty?
  {:doc      "True if empty or #{nil}, else false."
   :fdb/spec nil
   :fdb/cost 10}
  [?ctx arg]
  (go-try
    (let [arg   (extract arg)
          res   (fdb/empty? arg)
          entry [{:function "empty?" :arguments [arg] :result res} 10]]
      (add-stack ?ctx entry)
      res)))

(defn str
  {:doc      "Concatenates all in sequence."
   :fdb/spec nil
   :fdb/cost 10}
  [?ctx & args]
  (go-try
    (let [args  (<? (coerce-args args))
          res   (apply fdb/str args)
          entry [{:function "str" :arguments [args] :result res} 10]]
      (add-stack ?ctx entry)
      res)))

(defn subs
  {:doc      "Returns substring of a string with a start and optional end integer. Returned string is inclusive of start integer and exclusive of end integer."
   :fdb/spec nil
   :fdb/cost 30}
  [?ctx args]
  (go-try
    (let [args  (<? (coerce-args args))
          res   (apply fdb/subs args)
          entry [{:function "subs" :arguments [args] :result res} 30]]
      (add-stack ?ctx entry)
      res)))

(defn lower-case
  {:doc      "Makes string lower case"
   :fdb/spec nil
   :fdb/cost 10}
  [?ctx str]
  (go-try
    (let [str   (extract str)
          res   (fdb/lower-case str)
          entry [{:function "lower-case" :arguments [str] :result res} 10]]
      (add-stack ?ctx entry)
      res)))

(defn upper-case
  {:doc      "Makes string upper-case"
   :fdb/spec nil
   :fdb/cost 10}
  [?ctx str]
  (go-try
    (let [str   (extract str)
          res   (fdb/upper-case str)
          entry [{:function "upper-case" :arguments [str] :result res} 10]]
      (add-stack ?ctx entry)
      res)))

(defn ?sid
  {:doc      "Gets current subject id"
   :fdb/spec nil
   :fdb/cost 10}
  [?ctx]
  (cond (:sid ?ctx)
        (let [res   (:sid ?ctx)
              entry [{:function "?sid" :arguments "?ctx" :result res} 10]
              _     (add-stack ?ctx entry)] res)

        (clojure.core/and (:s ?ctx) (clojure.core/not (string? (clojure.core/get-in ?ctx [:s :_id]))))
        (let [res   (clojure.core/get-in ?ctx [:s :_id])
              entry [{:function "?sid" :arguments "?ctx" :result res} 10]]
          (add-stack ?ctx entry)
          res)

        :else
        (raise ?ctx "Cannot access ?sid from this function interface")))

(defn ?pid
  {:doc      "Gets current predicate id"
   :fdb/spec nil
   :fdb/cost 10}
  [?ctx]
  (cond (:pid ?ctx)
        (let [res   (:pid ?ctx)
              entry [{:function "?pid" :arguments "?ctx" :result res} 10]]
          (add-stack ?ctx entry)
          res)
        :else
        (raise ?ctx "Cannot access ?pid from this function interface")))

(defn ?s
  {:doc      "Gets current subject."
   :fdb/spec nil
   :fdb/cost "10 if no lookup necessary, 10 plus fuel cost if lookup necessary"}
  ([?ctx]
   (?s ?ctx nil))
  ([?ctx additional-select]
   (go-try
     (let [[res fuel] (<? (fdb/?s ?ctx additional-select))
           entry [{:function "?s" :arguments "?ctx" :result res} (clojure.core/+ 10 fuel)]]
       (add-stack ?ctx entry)
       res))))

(defn ?p
  {:doc      "Gets current predicate predicates"
   :fdb/spec nil
   :fdb/cost "10 if no lookup necessary, 10 plus fuel cost if lookup necessary"}
  ([?ctx]
   (?p ?ctx nil))
  ([?ctx additional-select]
   (go-try
     (if
       (:pid ?ctx)
       (let [[res fuel] (<? (fdb/?p ?ctx (<? (coerce-args additional-select))))
             entry [{:function "?p" :arguments "?ctx" :result res} (clojure.core/+ 10 fuel)]]
         (add-stack ?ctx entry)
         res)
       (raise ?ctx "Cannot access ?p from this function interface")))))

(defn and
  {:doc      "Returns true if all in a sequence are true, else returns false"
   :fdb/spec nil
   :fdb/cost "Count of objects in and"}
  [?ctx & args]
  (go-try
    (let [args  (<? (coerce-args args))
          res   (apply fdb/and args)
          cost  (clojure.core/count [args])
          entry [{:function "and" :arguments [args] :result res} cost]]
      (add-stack ?ctx entry)
      res)))

(defn or
  {:doc      "Returns true if any in the sequence are true, else returns false"
   :fdb/spec nil
   :fdb/cost "Count of objects in or"}
  [?ctx & args]
  (go-try
    (let [args  (<? (coerce-args args))
          res   (apply fdb/or args)
          cost  (clojure.core/count [args])
          entry [{:function "or" :arguments [args] :result res} cost]]
      (add-stack ?ctx entry)
      res)))

(defn count
  {:doc      "Returns the number of items in the collection. (count nil) returns 0.  Also works on strings, arrays, and Java Collections and Maps"
   :fdb/spec nil
   :fdb/cost "9 + count of objects in count"}
  [?ctx coll]
  (go-try
    (let [coll  (if (vector? coll) (<? (coerce-args coll))
                                   (extract coll))
          res   (clojure.core/count (remove clojure.core/nil? coll))
          cost  (clojure.core/+ 9 res)
          entry [{:function "count" :arguments coll :result res} cost]]
      (add-stack ?ctx entry)
      res)))

(defn get
  {:doc      "Gets a value from an subject."
   :fdb/spec nil
   :fdb/cost 10}
  [?ctx subject pred]
  (go-try
    (let [subject  (extract subject)
          pred     (extract pred)
          subject' (if (vector? subject)
                     (if (= 1 (clojure.core/count subject))
                       (first subject)
                       subject)
                     subject)
          res      (fdb/get subject' pred)
          entry    [{:function "get" :arguments [subject pred] :result res} 10]]
      (add-stack ?ctx entry)
      res)))

(defn ?o
  {:doc      "Gets the object of an predicate from the current subject."
   :fdb/spec nil
   :fdb/cost 1}
  [?ctx]
  (if (:o ?ctx)
    (let [res   (:o ?ctx)
          entry [{:function "?o" :arguments "?ctx" :result res} 1]]
      (add-stack ?ctx entry)
      res)
    (raise ?ctx "Cannot access ?o from this function interface")))

(defn ?pO
  {:doc      "Gets the most recent object of an predicate, as of the previous block"
   :fdb/spec nil
   :fdb/cost "10 plus fuel cost"}
  [?ctx]
  (go-try
    (if (clojure.core/and (:sid ?ctx) (:pid ?ctx))
      (let [[res fuel] (<? (fdb/?pO ?ctx))
            entry [{:function "?pO" :arguments "?ctx" :result res} (clojure.core/+ 10 fuel)]]
        (add-stack ?ctx entry)
        res)
      (raise ?ctx "Cannot access ?pO from this function interface"))))

(defn get-all
  {:doc      "Follows an subject down the provided path and returns a set of all matching subjects."
   :fdb/spec nil
   :fdb/cost "9 + length of path"}
  [?ctx subject path]
  (go-try
    (let [subject  (extract subject)
          path     (extract path)
          subject' (if (vector? subject)
                     (if (= 1 (clojure.core/count subject))
                       (first subject)
                       subject)
                     subject)
          res      (fdb/get-all subject' path)
          cost     (clojure.core/+ 9 (clojure.core/count path))
          entry    [{:function "get-all" :arguments [subject path] :result res} cost]]
      (add-stack ?ctx entry)
      res)))

(defn get-in
  [?ctx subject path]
  {:doc      "Returns the value of a nested structure"
   :fdb/spec nil
   :fdb/cost "Length of path"}
  [?ctx subject path]
  (go-try
    (let [subject (extract subject)
          path    (extract path)
          res     (fdb/get-in subject path)
          cost    (clojure.core/count path)
          entry   [{:function "get-in" :arguments [subject path] :result res} cost]]
      (add-stack ?ctx entry)
      res)))

(defn contains?
  [?ctx coll key]
  {:doc      "Returns true if key is present."
   :fdb/spec nil
   :fdb/cost 10}
  (go-try
    (let [coll  (extract coll)
          coll' (if (set? coll) coll (-> coll flatten set))
          key   (extract key)
          res   (fdb/contains? coll' key)
          entry [{:function "contains?" :arguments [coll' key] :result res} 10]]
      (add-stack ?ctx entry)
      res)))

(defn hash-set
  [?ctx & args]
  {:doc      "Returns a hash-set of values"
   :fdb/spec nil
   :fdb/cost "9 + count of items in hash-set"}
  (go-try
    (let [args  (<? (coerce-args args))
          args' (if (clojure.core/and (= 1 (clojure.core/count args)) (coll? (first args))) (first args) args)
          res   (apply fdb/hash-set args')
          cost  (clojure.core/+ 9 (clojure.core/count [args']))
          entry [{:function "hash-set" :arguments [args'] :result res} cost]]
      (add-stack ?ctx entry)
      res)))

(defn ==
  {:doc      "Return true if arguments in sequence equal each other."
   :fdb/spec nil
   :fdb/cost "9 + number of arguments."}
  [?ctx & args]
  (go-try
    (let [args  (<? (coerce-args args))
          res   (apply fdb/== args)
          cost  (clojure.core/+ 9 (clojure.core/count [args]))
          entry [{:function "==" :arguments [args] :result res} cost]]
      (add-stack ?ctx entry)
      res)))

(defn >
  {:doc      "Returns non-nil if nums are in monotonically decreasing order, otherwise false."
   :fdb/spec nil
   :fdb/cost "9 + number of arguments."}
  [?ctx & args]
  (go-try
    (let [args  (<? (coerce-args args))
          res   (apply fdb/> args)
          cost  (clojure.core/+ 9 (clojure.core/count [args]))
          entry [{:function ">" :arguments [args] :result res} cost]]
      (add-stack ?ctx entry)
      res)))

(defn <
  {:doc      "Returns non-nil if nums are in monotonically increasing order, otherwise false."
   :fdb/spec nil
   :fdb/cost "9 + number of arguments."}
  [?ctx & args]
  (go-try
    (let [args  (<? (coerce-args args))
          res   (apply fdb/< args)
          cost  (clojure.core/+ 9 (clojure.core/count [args]))
          entry [{:function "<" :arguments [args] :result res} cost]]
      (add-stack ?ctx entry)
      res)))

(defn <=
  {:doc      "Returns non-nil if nums are in monotonically non-decreasing order,\notherwise false."
   :fdb/spec nil
   :fdb/cost "9 + number of arguments."}
  [?ctx & args]
  (go-try
    (let [args  (<? (coerce-args args))
          res   (apply fdb/<= args)
          cost  (clojure.core/+ 9 (clojure.core/count [args]))
          entry [{:function "<=" :arguments [args] :result res} cost]]
      (add-stack ?ctx entry)
      res)))

(defn >=
  {:doc      "Returns non-nil if nums are in monotonically non-increasing order,\notherwise false."
   :fdb/spec nil
   :fdb/cost "9 + number of arguments."}
  [?ctx & args]
  (go-try
    (let [args  (<? (coerce-args args))
          res   (apply fdb/>= args)
          cost  (clojure.core/+ 9 (clojure.core/count [args]))
          entry [{:function ">=" :arguments [args] :result res} cost]]
      (add-stack ?ctx entry)
      res)))

(defn not=
  {:doc      "Returns true if two (or more) values are not equal."
   :fdb/spec nil
   :fdb/cost "9 + number of arguments."}
  [?ctx & args]
  (go-try
    (let [args  (<? (coerce-args args))
          res   (apply fdb/not= args)
          cost  (clojure.core/+ 9 (clojure.core/count [args]))
          entry [{:function "not=" :arguments [args] :result res} cost]]
      (add-stack ?ctx entry)
      res)))

(defn max
  {:doc      "Gets max value from a sequence."
   :fdb/spec nil
   :fdb/cost "9 + number of arguments."}
  [?ctx & args]
  (go-try
    (let [args  (<? (coerce-args args))
          res   (apply fdb/max args)
          cost  (clojure.core/+ 9 (clojure.core/count [args]))
          entry [{:function "max" :arguments [args] :result res} cost]]
      (add-stack ?ctx entry)
      res)))

(defn min
  {:doc      "Gets min value from a sequence."
   :fdb/spec nil
   :fdb/cost "9 + number of arguments."}
  [?ctx & args]
  (go-try
    (let [args  (<? (coerce-args args))
          res   (apply fdb/min args)
          cost  (clojure.core/+ 9 (clojure.core/count [args]))
          entry [{:function "min" :arguments [args] :result res} cost]]
      (add-stack ?ctx entry)
      res)))

(defn query
  {:doc      "Executes a query"
   :fdb/spec nil
   :fdb/cost "Fuel required for query"}
  ([?ctx query-map]
   (go-try
     (let [query-map (extract query-map)
           query-map (if (string? query-map)
                       (json/parse query-map)
                       query-map)
           q-res     (<? (fdb/query (:db ?ctx) query-map))
           [res fuel] q-res
           entry     [{:function "query" :arguments [query-map] :result res} fuel]]
       (add-stack ?ctx entry)
       res)))
  ([?ctx select from where block limit]
   (go-try
     (let [select (extract select)
           from   (extract from)
           where  (extract where)
           block  (extract block)
           limit  (extract limit)
           [res fuel] (<? (fdb/query (:db ?ctx) select from where block limit))
           entry  [{:function "query" :arguments [select from where block] :result res} fuel]]
       (add-stack ?ctx entry)
       res))))

(defn relationship?
  {:doc      "Determines whether there is a relationship between two subjects"
   :fdb/spec nil
   :fdb/cost "10, plus fuel cost"}
  [?ctx startSubject path endSubject]
  (go-try
    (let [startSubject (extract startSubject)
          path         (<? (coerce-args path))
          endSubject   (extract endSubject)
          [res fuel] (<? (fdb/relationship? (:db ?ctx) startSubject path endSubject))
          res          (if (clojure.core/empty? res) false true)
          entry        [{:function "relationship?" :arguments [startSubject path endSubject] :result res} fuel]]
      (add-stack ?ctx entry)
      res)))

(defn max-pred-val
  {:doc      "Finds the maximum predicate value."
   :fdb/spec nil
   :fdb/cost "10, plus fuel cost."}
  [?ctx pred-name]
  (go-try
    (let [pred-name (extract pred-name)
          [res fuel] (<? (fdb/max-pred-val (:db ?ctx) pred-name nil))
          entry     [{:function "max-pred-val" :arguments pred-name :result res} (clojure.core/+ fuel 10)]]
      (add-stack ?ctx entry)
      res)))

(defn inc
  {:doc      "Increments any number (or nil/null) by 1."
   :fdb/spec nil
   :fdb/cost 10}
  [?ctx n]
  (go-try
    (let [n     (extract n)
          res   (fdb/inc n)
          entry [{:function "inc" :arguments n :result res} 10]]
      (add-stack ?ctx entry)
      res)))

(defn dec
  {:doc      "Decrements any number (or nil/null) by 1."
   :fdb/spec nil
   :fdb/cost 10}
  [?ctx n]
  (go-try
    (let [n     (extract n)
          res   (fdb/dec n)
          entry [{:function "dec" :arguments n :result res} 10]]
      (add-stack ?ctx entry)
      res)))


(defn now
  {:doc      "Returns current epoch milliseconds on the executing machine."
   :fdb/spec nil
   :fdb/cost 10}
  [?ctx]
  (let [res   (.toEpochMilli (:instant ?ctx))
        entry [{:function "now" :arguments [] :result res} 10]]
    (add-stack ?ctx entry)
    res))

(defn +
  {:doc      "Returns sum of each argument."
   :fdb/spec nil
   :fdb/cost "9 + count of numbers in +"}
  [?ctx & args]
  (go-try
    (let [args  (<? (coerce-args args))
          res   (apply fdb/+ args)
          cost  (clojure.core/+ 9 (clojure.core/count [args]))
          entry [{:function "+" :arguments [args] :result res} cost]]
      (add-stack ?ctx entry)
      res)))

(defn -
  {:doc      "Returns difference of all the numbers in the sequence with the first number as the minuend."
   :fdb/cost "9 + count of numbers in -"
   :fdb/spec nil}
  [?ctx & args]
  (go-try
    (let [args  (<? (coerce-args args))
          res   (apply fdb/- args)
          cost  (clojure.core/+ 9 (clojure.core/count [args]))
          entry [{:function "-" :arguments [args] :result res} cost]]
      (add-stack ?ctx entry)
      res)))

(defn *
  {:doc      "Returns product of all the numbers in the sequence."
   :fdb/spec nil
   :fdb/cost "9 + count of numbers in *"}
  [?ctx & args]
  (go-try
    (let [args  (<? (coerce-args args))
          res   (apply fdb/* args)
          cost  (clojure.core/+ 9 (clojure.core/count [args]))
          entry [{:function "*" :arguments [args] :result res} cost]]
      (add-stack ?ctx entry)
      res)))

(defn /
  {:doc      "If no denominators are supplied, returns 1/numerator, else returns numerator divided by all of the denominators. Takes a sequence"
   :fdb/spec nil
   :fdb/cost "9 + count of numbers in /"}
  [?ctx & args]
  (go-try
    (let [args  (<? (coerce-args args))
          res   (apply fdb// args)
          cost  (clojure.core/+ 9 (clojure.core/count [args]))
          entry [{:function "/" :arguments [args] :result res} cost]]
      (add-stack ?ctx entry)
      res)))

(defn quot
  {:doc      "Quot[ient] of dividing numerator by denominator."
   :fdb/spec nil
   :fdb/cost 10}
  [?ctx n d]
  (go-try
    (let [n     (extract n)
          d     (extract d)
          res   (fdb/quot n d)
          entry [{:function "quot" :arguments [n d] :result res} 2]]
      (add-stack ?ctx entry)
      res)))

(defn mod
  {:doc      "Modulus of num and div. Truncates toward negative infinity."
   :fdb/spec nil
   :fdb/cost 10}
  [?ctx n d]
  (go-try
    (let [n     (extract n)
          d     (extract d)
          res   (fdb/mod n d)
          entry [{:function "mod" :arguments [n d] :result res} 2]]
      (add-stack ?ctx entry)
      res)))

(defn rem
  {:doc      "Remainder of dividing numerator by denominator."
   :fdb/spec nil
   :fdb/cost 10}
  [?ctx n d]
  (go-try
    (let [n     (extract n)
          d     (extract d)
          res   (fdb/rem n d)
          entry [{:function "rem" :arguments [n d] :result res} 2]]
      (add-stack ?ctx entry)
      res)))

(defn boolean
  {:doc      "Coerce to boolean. Everything except false and nil is true."
   :fdb/spec nil
   :fdb/cost 10}
  [?ctx x]
  (go-try
    (let [x     (extract x)
          res   (fdb/boolean x)
          entry [{:function "boolean" :arguments x :result res} 10]]
      (add-stack ?ctx entry)
      res)))

(defn re-find
  {:doc      "Execute a re-find operation of regex pattern on provided string."
   :fdb/spec nil
   :fdb/cost 10}
  [?ctx pattern string]
  (go-try
    (let [pattern (extract pattern)
          string  (extract string)
          res     (fdb/re-find pattern string)
          entry   [{:function "re-find" :arguments [pattern string] :result res} 10]]
      (add-stack ?ctx entry)
      res)))

(defn valid-email?
  {:doc      "Determines whether an email is valid, based on its pattern"
   :fdb/spec nil
   :fdb/cost 10}
  [?ctx email]
  (go-try
    (let [email (extract email)
          res   (fdb/valid-email? email)
          entry [{:function "re-find" :arguments email :result res} 10]]
      (add-stack ?ctx entry)
      res)))

(defn ?user_id
  {:doc      "Gets current user _id."
   :fdb/spec nil
   :fdb/cost "10 if no lookup necessary, 10 plus fuel cost if lookup necessary"}
  [?ctx]
  (go-try
    (let [[res fuel] (cond (:user_id ?ctx)
                           [(:user_id ?ctx) 0]

                           (:auth_id ?ctx)
                           (<? (fdb/?user_id-from-auth ?ctx))

                           :else
                           (raise ?ctx "Cannot access ?user_id from this function interface"))
          entry [{:function "?user_id" :arguments "?ctx" :result res} (clojure.core/+ 10 fuel)]]
      (add-stack ?ctx entry)
      res)))

(defn ?auth_id
  {:doc      "Gets current auth _id."
   :fdb/spec nil
   :fdb/cost 10}
  [?ctx]
  (go-try (if (:auth_id ?ctx)
            (let [res   (<? (fdb/?auth_id ?ctx))
                  entry [{:function "?auth_id" :arguments "?ctx" :result res} 10]]
              (add-stack ?ctx entry)
              res)
            (raise ?ctx "Cannot access ?auth_id from this function interface"))))

(defn objT
  {:doc      "Gets the summed object of all true flakes"
   :fdb/spec nil
   :fdb/cost 10}
  [?ctx]
  (if (:flakes ?ctx)
    (let [res   (fdb/objT (:flakes ?ctx))
          entry [{:function "objT" :arguments (:flakes ?ctx) :result res} 10]]
      (add-stack ?ctx entry)
      res)
    (raise ?ctx "Cannot access flakes, or use objT function from this function interface")))

(defn objF
  {:doc      "Gets the summed object of all false flakes"
   :fdb/spec nil
   :fdb/cost 10}
  [?ctx]
  (if (:flakes ?ctx)
    (let [res   (fdb/objF (:flakes ?ctx))
          entry [{:function "objF" :arguments (:flakes ?ctx) :result res} 10]]
      (add-stack ?ctx entry)
      res)
    (raise ?ctx "Cannot access flakes, or use objF function from this function interface")))

(defn flakes
  {:doc      "Gets the flakes from the current subject."
   :fdb/spec nil
   :fdb/cost 10}
  [?ctx]
  (if (:flakes ?ctx)
    (let [res   (:flakes ?ctx)
          entry [{:function "flakes" :arguments "?ctx" :result res} 10]]
      (add-stack ?ctx entry)
      res)
    (raise ?ctx "Cannot access flakes from this function interface")))

(defn rand
  {:doc      "Returns a random number, seed is either provided or a we use the txn instant"
   :fdb/spec nil
   :fdb/cost 10}
  ([?ctx max]
   (rand ?ctx max (.toEpochMilli (:instant ?ctx))))
  ([?ctx max seed]
   (go-try
     (let [seed' (extract seed)
           max'  (clojure.core/or (extract max) 10)
           res   (fdb/rand seed' max')
           entry [{:function "rand" :arguments [max seed] :result res} 10]]
       (add-stack ?ctx entry)
       res))))

(defn uuid
  {:doc      "Returns a random number, seed is either provided or a we use the txn instant"
   :fdb/spec nil
   :fdb/cost 10}
  [?ctx]
  (go-try (let [res   (clojure.core/str (util/random-uuid))
                entry [{:function "uuid" :arguments [] :result res} 10]]
            (add-stack ?ctx entry)
            res)))

(defn ceil
  {:doc      "Takes the ceiling of a number"
   :fdb/spec nil
   :fdb/cost 10}
  [?ctx num]
  (go-try
    (let [num   (extract num)
          res   (fdb/ceil num)
          entry [{:function "" :arguments [ceil] :result res} 10]]
      (add-stack ?ctx entry)
      res)))

(defn floor
  {:doc      "Takes the floor of a number"
   :fdb/spec nil
   :fdb/cost 10}
  [?ctx num]
  (go-try
    (let [num   (extract num)
          res   (fdb/floor num)
          entry [{:function "" :arguments [floor] :result res} 10]]
      (add-stack ?ctx entry)
      res)))

(defn cas
  {:doc      "Does a compare and set/swap operation as a transaction function."
   :fdb/spec nil
   :fdb/cost 20}
  [?ctx compare-val new-val]
  (go-try
    (let [res   (<? (fdb/cas ?ctx compare-val new-val))
          entry [{:function "" :arguments [compare-val new-val] :result res} 10]]
      (add-stack ?ctx entry)
      res)))
