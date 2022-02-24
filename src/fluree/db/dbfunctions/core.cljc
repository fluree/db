(ns fluree.db.dbfunctions.core
  (:require [sci.core :as sci]
            [fluree.db.util.async :refer [go-try <? channel?]]
            [fluree.db.dbfunctions.fns]
            [fluree.db.util.log :as log]))

(defn clear-db-fn-cache
  [])
  ;; TODO: Implement this if we end needing a db fn cache w/ SCI

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
    (ns-publics (quote ns))))


(def sci-ctx
  (delay
    (sci/init {:namespaces {'clojure.core
                            (load-ns 'fluree.db.dbfunctions.fns)}})))


(defn combine-fns
  "Given a collection of function strings, returns a combined function using
  the and function."
  [fn-str-coll])
;; TODO: Implement me


;; TODO: This needs to be recursive b/c fn calls can be nested
;; good use case for clojure.walk? or maybe just plain ol' recursion?
(defn- wrap-fn
  [fn-form]
  `(fn [~'?ctx] (~(first fn-form) ~'?ctx ~@(rest fn-form))))

;; TODO: Need to implement some of the checks from the old ns here
;; e.g. symbol-whitelist, comparing symbols against args, etc.
;; try to combine the list and vector processing
(defn parse-fn
  ([db fn-str type] (parse-fn db fn-str type nil))
  ([db fn-str type params]
   (case fn-str
     ("true" "false") (sci/parse-string @sci-ctx (str "(fn [_] " fn-str ")"))
     (if-not (re-matches #"^\(.+\)$" fn-str)
       (throw (ex-info "Bad function"
                       {:status 400
                        :error  :db/invalid-fn}))
       ;; TODO: Load & cache db fns too
       (let [parsed-fn      (sci/parse-string @sci-ctx fn-str)
             wrapped-fn     (wrap-fn parsed-fn)]
         (log/trace "Parsed & wrapped db fn:" wrapped-fn)
         wrapped-fn)))))


(defn execute-tx-fn
  "Executes a transaction function"
  [{:keys [db auth _credits s p o fuel block-instant]}]
  (go-try
    (let [fn-str (subs o 1) ; remove preceding '#'
          ctx    {:db      db
                  :instant block-instant
                  :sid     s
                  :pid     p
                  :auth_id auth
                  :state   fuel}
          f      (parse-fn db fn-str "txn")
          res    (sci/eval-form @sci-ctx (list f ctx))]
      (if (channel? res)
        (<? res)
        res))))
