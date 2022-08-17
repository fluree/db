(ns fluree.db.dbfunctions.core
  (:require [sci.core :as sci]
            [fluree.db.dbfunctions.fns]
            [fluree.db.dbproto :as dbproto]
            [fluree.db.util.core #?(:clj :refer :cljs :refer-macros) [condps]]
            [fluree.db.util.async :refer [go-try <? channel?]]
            [fluree.db.util.log :as log :include-macros true]
            [clojure.string :as str]))

(defn tx-fn?
  "Returns true if the arg is a string containing a transaction function."
  [v]
  (and (string? v) (re-matches #"^#\(.+\)$" v)))

(def allowed-symbols #{'?s '?user_id '?db '?o 'sid '?auth_id '?pid '?a '?pO})

(defn load-local-fns-ns
  "Copies local fns ns public vars into SCI"
  []
  ;; Unfortunately the ns below has to be a quoted symbol. If you can figure out
  ;; how to make it anything else (e.g. a let var or an arg to this fn), please
  ;; do. Just make sure it works in both CLJ and CLJS. I went round and round
  ;; with it for quite some time. This is due to the assert in the CLJS version
  ;; of `ns-publics`. I tried making it work with a macro instead but couldn't
  ;; figure it out.
  ;;   - WSM 2022-04-19
  (let [ns-public-vars (ns-publics 'fluree.db.dbfunctions.fns)
        _              (log/debug (str "Loading " (count ns-public-vars) " local fns"))
        sci-ns         (sci/create-ns 'fluree.db.dbfunctions.fns)]
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
                           (cond-> {:ns   sci-ns
                                    :name (:name m)}
                                   (:macro m) (assoc :macro true)
                                   doc (assoc :doc doc)
                                   arglists (assoc :arglists arglists)))))))
      {}
      ns-public-vars)))

(def sci-ctx
  (delay
    (let [cfg {:namespaces {'fluree.db.dbfunctions.fns (load-local-fns-ns)}}]
      (log/debug "SCI config:" cfg)
      (sci/init cfg))))

(defn parse-string [s]
  (sci/parse-string @sci-ctx s))

(defn eval-string [s]
  (sci/eval-string* @sci-ctx s))

(defn eval-form [f]
  (sci/eval-form @sci-ctx f))

(defn combine-fns
  "Given a collection of function strings, returns a combined function using
  the and function."
  [fn-str-coll]
  (if (> (count fn-str-coll) 1)
    (str "(and " (str/join " " fn-str-coll) ")")
    (first fn-str-coll)))

(defn find-local-fn
  "Tries to resolve a local pre-defined fn with fn-name. Returns a fn-map if
  found, nil otherwise."
  [fn-name]
  (log/debug "Looking for local fn:" fn-name)
  (let [local-fns-ns 'fluree.db.dbfunctions.fns
        sci-vars     (eval-string (str "(ns-publics '" local-fns-ns ")"))]
    (log/debug "SCI vars:" sci-vars)
    (when ((-> sci-vars keys set) (symbol fn-name))
      (log/debug "Found local fn:" fn-name)
      (let [fn-var (symbol (str local-fns-ns) (str fn-name))

            {:keys [arglists fdb/spec] :as sci-fn-meta}
            (eval-string (str "(meta #'" fn-var ")"))]
        (when sci-fn-meta
          (log/debug "SCI fn metadata:" sci-fn-meta)
          (let [first-arglist (first arglists)
                args-set      (set first-arglist)
                var-args?     (boolean (args-set '&))
                arity         (when-not var-args?
                                (set (map #(-> % count dec) arglists)))]
            {:f         fn-var
             :params    arglists
             :arity     arity
             :var-args? var-args?
             :spec      spec}))))))

(declare validate-form)

(defn find-db-fn
  ([db fn-name] (find-db-fn db fn-name nil))
  ([db fn-name fn-type]
   (go-try
     ;; TODO: Implement caching here if necessary
     (log/debug "Looking for custom db fn:" fn-name)
     (let [query           {:selectOne ["_fn/params" "_fn/code" "_fn/spec"]
                            :from      ["_fn/name" (name fn-name)]}
           res             (<? (dbproto/-query db query))
           _               (when (empty? res)
                             (throw
                               (ex-info (str "Unknown function: "
                                             (pr-str fn-name))
                                        {:status 400, :error :db/invalid-fn})))
           _               (log/debug "Custom db fn query results:" res)
           params          (some-> res (get "_fn/params") parse-string)
           _               (log/debug "Parsed params:" params)
           code            (<? (validate-form db (parse-string
                                                   (get res "_fn/code"))
                                              fn-type params))
           _               (log/debug "Validated code:" code)
           spec            (get res "_fn/spec")
           params-with-ctx (->> params
                                (mapv symbol)
                                (cons '?ctx)
                                (into []))
           custom-fn       (parse-string (str "(fn " params-with-ctx
                                              " " code ")"))]
       (log/debug "Found custom db fn:" (pr-str custom-fn))
       {:f         custom-fn
        :params    params
        :arity     (hash-set (count params))
        :var-args? false
        :spec      spec
        :code      nil}))))

(defn valid-symbol?
  "Is the symbol sym valid with the given form type & params?"
  [type params sym]
  (or (allowed-symbols sym)
      ((->> params (map symbol) set) sym)
      (= type "functionDec")))

(defn resolve-fn
  "Resolves local or custom db-stored fn from fn-name string"
  [db fn-name type]
  (go-try
    (or (find-local-fn fn-name)
        (<? (find-db-fn db fn-name type)))))

(defn validate-form
  ([db form] (validate-form db form nil nil))
  ([db form type] (validate-form db form type nil))
  ([db form type params]
   (go-try
     (log/debug "Validating form:" form "- params:" params)
     (let [fn-name (when (list? form) (first form))
           args    (if fn-name (rest form) form)
           args-n  (count args)
           fn-map  (when fn-name (<? (resolve-fn db fn-name type)))
           {:keys [f arity var-args?]} fn-map
           args*   (when (seq args)
                     (loop [[arg & r] args
                            acc []]
                       (log/debug "Validating arg:" arg)
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
                           acc*))))]
       (if fn-name
         (let [validated-form (cons f (cons '?ctx args*))]
           (log/debug "Validated fn form:" validated-form)
           validated-form)
         (do
           (log/debug "Validated non-fn form:" form)
           form))))))

(defn parse-fn
  [db fn-str type params]
  (go-try
    (case fn-str
      ("true" "false") (parse-string (str "(fn [_] " fn-str ")"))
      (if (re-matches #"^\(.+\)$" fn-str)
        (let [parsed-fn    (parse-string fn-str)
              validated-fn (<? (validate-form db parsed-fn type params))]
          (log/debug "Parsed & validated db fn:" validated-fn)
          validated-fn)
        (throw (ex-info "Bad function"
                        {:status 400
                         :error  :db/invalid-fn}))))))

(defn parse-and-wrap-fn
  ([db fn-str type] (parse-and-wrap-fn db fn-str type nil))
  ([db fn-str type params]
   (go-try
     (let [parsed  (<? (parse-fn db fn-str type params))
           wrapped `(fn [~'?ctx] ~parsed)
           f       (if (and params (= type "functionDec"))
                     wrapped
                     (eval-form wrapped))]
       (with-meta f {:fnstr fn-str})))))

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
          f-wrapped (<? (parse-and-wrap-fn db fn-str "txn"))
          f-call    (list f-wrapped ctx)
          _         (log/debug "Evaluating fn call:" f-call)
          res       (eval-form f-call)]
      (if (channel? res)
        (<? res)
        res))))
