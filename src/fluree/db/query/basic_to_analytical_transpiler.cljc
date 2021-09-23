(ns fluree.db.query.basic-to-analytical-transpiler
  (:require [fluree.db.query.fql-parser :as fql-parser]
            [clojure.string :as str]
            [fluree.db.util.core :as util]
            #?(:clj  [clojure.core.async :refer [go <!] :as async]
               :cljs [cljs.core.async :refer [go <!] :as async])
            [fluree.db.util.async :refer [<? go-try]]))

#?(:clj (set! *warn-on-reflection* true))


(defn basic-to-analytical-transpiler
  [query-map opts]
  (let [{:keys [select selectOne selectDistinct where from limit offset component orderBy]} query-map
        selectKey  (cond select :select
                         selectOne :selectOne
                         selectDistinct :selectDistinct)
        select-smt (or select selectOne selectDistinct)]
    (-> (cond
          ;; String-based where
          (string? where)
          (throw (ex-info (str "Queries with where strings are not supported by the basic->analytical transpiler")
                          {:status 400
                           :error  :db/invalid-query}))

          ;; Predicate-based query
          (and (string? from) (str/includes? #?(:clj from :cljs (str from)) "/"))
          {selectKey {"?s" select-smt}
           :where    [["?s" from "?o"]
                      ["?s" "rdf:type" from]]
           :limit    limit
           :offset   offset}

          ;; Collection-based query
          (string? from)
          (let [selectKey  (cond select :select
                                 selectOne :selectOne
                                 selectDistinct :selectDistinct)
                select-smt (or select selectOne selectDistinct)]
            {selectKey {"?s" select-smt}
             :where    [["?s" "rdf:type" from]]})

          ;; TODO - output will not be structured the same, but the data will be there. Need to convert from triples to JSON-LD
          ; Single subject - subject _id
          (number? from)
          {:select ["?s" "?p" "?o"]
           :where  [["?s" from]
                    ["?s" "?p" "?o"]]}

          ; Single subject - two-tuple
          (and (vector? from) (= 2 (count from)))
          {:select ["?s" "?p" "?o"]
           :where  [["?s" (first from) (second from)]
                    ["?s" "?p" "?o"]]}


          ;;; multiple subject ids provided
          (and (sequential? from) (every? util/subj-ident? from))
          (let [union (reduce (fn [acc from-itm]
                                (cond (number? from-itm)
                                      (conj acc [["?s" from-itm] ["?s" "?p" "?o"]])


                                      (and (vector? from-itm) (= 2 (count from-itm)))
                                      (conj acc [["?s" (first from-itm) (second from-itm)] ["?s" "?p" "?o"]])

                                      :else
                                      (throw (ex-info (str "Invalid subject in from. Provided: " from-itm)
                                                      {:status 400
                                                       :error  :db/invalid-query}))))
                              [] from)]
            {selectKey ["?s" "?p" "?o"]
             :where    [{:union union}]})

          :else
          (ex-info (str "Invalid 'from' in query:" (pr-str query-map))
                   {:status 400 :error :db/invalid-query}))
        (assoc :limit limit :offset offset))))



(comment
  (def db (clojure.core.async/<!! (fluree.db.api/db (:conn user/system) "fluree/test")))

  (def qy (basic-to-analytical-transpiler {:select ["*"] :from [439804651110402, 351843720888321, ["person/handle", "jdoe"]]} {}))

  qy

  (async/<!! (fluree.db.api/query-async (fluree.db.api/db (:conn user/system) "fluree/test")
                                        qy))

  (conj [1 2 3] 1 2))



