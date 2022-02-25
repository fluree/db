(ns fluree.db.dbfunctions.core
  (:require [sci.core :as sci]
            [fluree.db.dbfunctions.fns]
            [fluree.db.util.core :refer [condps]]
            [fluree.db.util.async :refer [go-try <? channel?]]
            [fluree.db.util.log :as log]))

(defn clear-db-fn-cache
  [])
;; TODO: Implement this if we end needing a db fn cache w/ SCI

(def symbol-whitelist #{'?s '?user_id '?db '?o 'sid '?auth_id '?pid '?a '?pO})

(defmacro ns-public-vars
  "ClojureScript gets cranky if the arg to ns-publics isn't a quoted symbol
  literal at runtime. So we need this macro to make it chill out."
  [ns]
  `(ns-publics ~(quote ns)))

(defn load-ns
  "Copies public vars in ns into SCI"
  [ns]
  (reduce
    (fn [ns-map [var-name var]]
      (let [m        (meta var)
            no-doc   (:no-doc m)
            doc      (:doc m)
            arglists (:arglists m)]
        (if no-doc
          ns-map
          (assoc ns-map
            var-name
            (sci/new-var (symbol var-name) @var
                         (cond-> {:ns   (sci/create-ns ns)
                                  :name (:name m)}
                                 (:macro m) (assoc :macro true)
                                 doc (assoc :doc doc)
                                 arglists (assoc :arglists arglists)))))))
    {}
    (ns-public-vars ns)))


(def sci-ctx
  (delay
    (sci/init {:namespaces {'clojure.core
                            (load-ns 'fluree.db.dbfunctions.fns)}})))


(defn combine-fns
  "Given a collection of function strings, returns a combined function using
  the and function."
  [fn-str-coll])
;; TODO: Implement me


(defn find-local-fn
  [fn-name]
  ;; TODO: Implement the rest of the fn-map return value
  (let [{:keys [arglists] :as sci-fn-meta}
        (sci/eval-string* @sci-ctx (str "(meta #'" fn-name ")"))]
    (when sci-fn-meta
      (let [first-arglist (first arglists)
            args-set      (set first-arglist)
            var-args?     (boolean (args-set '&))
            arity         (when-not var-args?
                            (set (map #(-> % count dec) arglists)))]
        {:f         fn-name
         :arity     arity
         :var-args? var-args?}))))

(defn find-db-fn
  ([db fn-name] (find-db-fn db fn-name nil))
  ([db fn-name fn-type]
   (go-try
     ;; TODO: Implement caching here if necessary
     (let [query {:selectOne ["_fn/params" "_fn/code" "_fn/spec"]
                  :from      ["_fn/name" (name fn-name)]}]
       ;; TODO: Finish this
       {:f nil}))))

(defn valid-symbol?
  "Is the symbol sym valid with the given form type & params?"
  [type params sym]
  (or (symbol-whitelist sym)
      ((set params) sym)
      (= type "functionDec")))

(defn validate-form
  ([db form] (validate-form db form nil nil))
  ([db form type] (validate-form db form type nil))
  ([db form type params]
   (go-try
     (let [fn-name (when (list? form) (first form))
           args    (if fn-name (rest form) form)
           args-n  (count args)
           fn-map  (when fn-name
                     (or (find-local-fn fn-name)
                         (<? (find-db-fn db fn-name type))))
           {:keys [f arity arglist var-args?]} fn-map
           args*   (loop [[arg & r] args
                          acc []]
                     (let [arg* (condps arg
                                  (list? vector?)
                                  (<? (validate-form db arg type params))

                                  (string? number? true? false? nil?) arg

                                  #(and (symbol? %)
                                        (valid-symbol? type params %)) arg

                                  (throw
                                    (ex-info (str "Invalid element ("
                                                  (pr-str arg) (type arg)
                                                  ") in form: " (pr-str form)
                                                  ".")
                                             {:status 400
                                              :error  :db/invalid-fn})))
                           acc* (conj acc arg*)]
                       (if (seq r)
                         (recur r acc*)
                         acc*)))]
       (if fn-name
         (cons f (cons '?ctx args*))
         form)))))


(defn parse-fn
  ([db fn-str type] (parse-fn db fn-str type nil))
  ([db fn-str type params]
   (go-try
     (case fn-str
       ("true" "false") (sci/parse-string @sci-ctx (str "(fn [_] " fn-str ")"))
       (if-not (re-matches #"^\(.+\)$" fn-str)
         (throw (ex-info "Bad function"
                         {:status 400
                          :error  :db/invalid-fn}))
         ;; TODO: Load & cache db fns too
         (let [parsed-fn    (sci/parse-string @sci-ctx fn-str)
               validated-fn (<? (validate-form db parsed-fn type params))]
           (log/trace "Parsed & validated db fn:" validated-fn)
           validated-fn))))))

(defn execute-tx-fn
  "Executes a transaction function"
  [{:keys [db auth _credits s p o fuel block-instant]}]
  (go-try
    (let [fn-str    (subs o 1) ; remove preceding '#'
          ctx       {:db      db
                     :instant block-instant
                     :sid     s
                     :pid     p
                     :auth_id auth
                     :state   fuel}
          f         (<? (parse-fn db fn-str "txn"))
          f-wrapped `(fn [~'?ctx] ~f)
          _         (log/debug "Evaluating wrapped fn:" f-wrapped)
          res       (sci/eval-form @sci-ctx (list f-wrapped ctx))]
      (if (channel? res)
        (<? res)
        res))))
