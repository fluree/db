(ns fluree.db.query.graphql-parser
  (:require [clojure.string :as str]
            [#?(:cljs cljs.cache :clj clojure.core.cache) :as cache]
            [fluree.db.util.core :as util :refer [if-cljs try* catch*]]
            [fluree.db.query.fql :as fql]
            [fluree.db.util.async :refer [<? go-try]]
            #?(:cljs [cljs.reader :refer [read-string]])))

(def collection-arguments
  {:limit {:type "Int" :default-value 100}
   :_id   {:type "ID"}
   :ident {:type "Ident"}
   :sort  {:type "PredicateSort"}
   :recur {:type "Int"}
   :where {:type "String"}})

(def output-flake-type
  {:fields {:p  {:type "String"}
            :s  {:type "String"}
            :m  {:type "JSON"}
            :op {:type "Boolean"}
            :t  {:type "Long"}
            :o  {:type "JSON"}}})

(defn flake-fields
  [name]
  {:name              (util/keyword->str name)
   :args              []
   :deprecationReason nil
   :description       nil
   :isDeprecated      false
   :type              {:kind   "SCALAR"
                       :name   (get-in output-flake-type [:fields name :type])
                       :ofType nil}})

(defn format-types
  [type-obj]
  {:description       (:description type-obj)
   :enumValues        (or (:enumValues type-obj) [])
   :fields            (or (:fields type-obj) [])
   :inputFields       (or (:inputFields type-obj) [])
   :interfaces        (or (:interface type-obj) [])
   :kind              (:kind type-obj)
   :name              (:name type-obj)
   :type              (:type type-obj)
   :ofType            (:ofType type-obj)
   :args              (or (:args type-obj) [])
   :defaultValue      (:defaultValue type-obj)
   :deprecationReason (:deprecationReason type-obj)
   :isDeprecated      (or (:isDeprecated type-obj) false)
   :possibleTypes     (or (:possibleTypes type-obj) [])})


(def base-types
  ["BigDec" "BigInt" "Boolean" "Double" "Float" "ID" "Instant" "Int" "JSON" "Long" "String" "Ident"])

(defn format-scalar-types
  [name]
  {:name name
   :kind "SCALAR"})

(def base-types-formatted
  (map format-scalar-types base-types))

(def input-object-keys (format-types {:name        "PredicateSort"
                                      :kind        "INPUT_OBJECT"
                                      :inputFields [(format-types {:name "predicate"
                                                                   :type (format-scalar-types "String")})
                                                    (format-types {:name "order"
                                                                   :type {:kind   "ENUM"
                                                                          :name   "SortOrder"
                                                                          :ofType nil}})]}))
(def flake
  (format-types {:name   "Flake"
                 :kind   "OBJECT"
                 :fields (mapv
                           flake-fields
                           (keys (:fields output-flake-type)))}))

(defn format-collection-arguments
  [arg]
  {:name         (util/keyword->str arg)
   :description  (get-in collection-arguments [arg :description])
   :type         {:kind   (if (= "PredicateSort" (get-in collection-arguments [arg :type]))
                            "INPUT_OBJECT"
                            "SCALAR")
                  :name   (get-in collection-arguments [arg :type])
                  :ofType nil}
   :defaultValue (if-not (nil? (get-in collection-arguments [arg :default-value]))
                   (str (get-in collection-arguments [arg :default-value]))
                   nil)})

(defn collection-as-graph-fields
  [collection]
  {:name         (util/keyword->str collection)
   :args         (mapv
                   format-collection-arguments
                   (keys collection-arguments))
   :description  (str "Collection for: " collection)
   :isDeprecated false
   :type         {:kind   "LIST"
                  :name   nil
                  :ofType {:kind   "OBJECT"
                           :name   (util/keyword->str collection)
                           :ofType nil}}})

(defn graph-schema
  [collection-vec]
  (format-types {:name   "Graph"
                 :kind   "OBJECT"
                 :fields (mapv collection-as-graph-fields
                               collection-vec)}))

(def mutationRoot
  (format-types {:description "Root of all mutations."
                 :fields      [(format-types {:name "transact"
                                              :args [{:name        "tx"
                                                      :description nil
                                                      :type        (format-scalar-types "JSON")}]
                                              :type (format-scalar-types "JSON")})]
                 :name        "MutationRoot"
                 :kind        "OBJECT"}))

(def queryRoot
  (format-types {:description "Root of all queries."
                 :kind        "OBJECT"
                 :name        "QueryRoot"
                 :fields      [(format-types {:name "block"
                                              :args [(format-types {:name "from"
                                                                    :type (format-scalar-types "Int")})
                                                     (format-types {:name "to"
                                                                    :type (format-scalar-types "Int")})]
                                              :type {:kind   "LIST"
                                                     :ofType (format-scalar-types "JSON")}})

                               (format-types {:name "history"
                                              :args [(format-types {:name "entity"
                                                                    :type (format-scalar-types "String")})
                                                     (format-types {:name "block"
                                                                    :type (format-scalar-types "Int")})]
                                              :type {:kind   "LIST"
                                                     :ofType (format-scalar-types "JSON")}})

                               (format-types {:name "graph"
                                              :args [(format-types {:name "block" :type (format-scalar-types "Int")})]
                                              :type {:kind   "OBJECT"
                                                     :name   "Graph"
                                                     :ofType nil}})]}))

(def sortOrder
  (format-types {:description "Sort order, ASCending or DESCending"
                 :enumValues  [(format-types {:name "ASC"}) (format-types {:name "DESC"})]
                 :kind        "ENUM"
                 :name        "SortOrder"}))

(def graphql-type-map {:string  "String"
                       ;; floating point
                       :float   "Float"
                       :double  "Double"
                       :bigdec  "BigDec"
                       ;; integers
                       :int     "Int"
                       :long    "Long"
                       :bigint  "BigInt"
                       ;; instant
                       :instant "Instant"
                       ;; boolean
                       :boolean "Boolean"
                       ;; other
                       :tag     "String"
                       :uuid    "String"
                       :uri     "String"
                       :bytes   "String"
                       :json    "JSON"
                       :geojson "JSON"})

(defn collection-predicate-types
  [collection-map]
  (let [_id          (format-types {:name        "_id"
                                    :description "FlureeDB unique subject identifier."
                                    :type        (format-scalar-types "Long")})
        other-preds  (mapv (fn [k]
                             (let [type   (:_predicate/type k)
                                   type*  (if (= "ref" type)
                                            (let [restrictCollection (or (:_predicate/restrictCollection k)
                                                                         (throw (ex-info (str "GraphQL requires fully-formed schema. All reference types must have a restrictCollection _predicate. The predicate: " (:_predicate/name k)
                                                                                              " with _id: " (:_id k) " does not have an _predicate/restrictCollection")
                                                                                         {:status 400
                                                                                          :error  :db/invalid-query})))]
                                              {:kind "OBJECT" :name (util/keyword->str restrictCollection) :ofType nil})
                                            {:kind "SCALAR" :name (get graphql-type-map (keyword type)) :ofType nil})
                                   multi? (:_predicate/multi k)
                                   type** (if multi?
                                            {:kind "LIST" :name nil :ofType type*}
                                            type*)
                                   args   (if (= "ref" type)
                                            (mapv
                                              format-collection-arguments
                                              (keys collection-arguments)) [])
                                   pred   (format-types {:name        (-> k
                                                                          :_predicate/name
                                                                          keyword
                                                                          name)
                                                         :description (:_predicate/doc k)
                                                         :args        args
                                                         :type        type**})]
                               pred)) (val collection-map))
        other-preds* (into [] (remove nil? other-preds))]
    (into [] (concat [_id] other-preds*))))


(defn collection-types
  [collection-map]
  {:kind          "OBJECT"
   :name          (util/keyword->str (key collection-map))
   :description   nil
   :fields        (collection-predicate-types collection-map)
   :inputFields   []
   :interfaces    []
   :enumValues    []
   :possibleTypes []})

(defn add-reverse-joins
  [schema]
  (reduce
    (fn [schema predicates]
      ;; only process refs with restrictCollection, and ensure the restricted collection exists in the main schema
      (let [refs (->> predicates
                      (filter #(and (:_predicate/restrictCollection %)
                                    (= "ref" (:_predicate/type %))
                                    (get schema (keyword (:_predicate/restrictCollection %))))))]
        (reduce
          (fn [schema ref]
            (let [restricted-collection (:_predicate/restrictCollection ref)
                  ref-name              (:_predicate/name ref)
                  reverse-ref-name      (str/replace ref-name "/" "_Via_")
                  original-collection   (util/keyword->str (:collection ref))]
              (update-in schema [(keyword restricted-collection)] conj
                         {:_predicate/name               reverse-ref-name
                          :_predicate/doc                (str "Reverse reference. " (:_predicate/doc ref))
                          :_predicate/restrictCollection original-collection
                          :_predicate/multi              (if (:_predicate/component ref)
                                                           false ;; components will always only refer to a single parent. Else always multi
                                                           true)
                          :_predicate/type               "ref"
                          :reverse-ref                   true ;; this flag used downcollection to know if this was a reverse ref
                          :collection                    (:collection ref)
                          :predicate                     (keyword reverse-ref-name)})))
          schema refs)))
    schema schema))

;; TODO - use the new cached schema on the DB
(defn pull-schema
  "Queries for schema. Format should be:
  {:collection-name [{}"
  [db opts]
  (go-try
    (let [meta?     (:meta opts)
          max-fuel  (:fuel opts)
          fuel      (when (or max-fuel meta?)
                      (volatile! 0))
          opts*     (assoc opts :fuel fuel
                                :max-fuel max-fuel)
          all-preds (<? (fql/query db {:select ["*"]
                                       :from   "_predicate"
                                       :opts   opts*}))]
      (let [schema (->>
                     all-preds
                     (mapv #(let [[collection-name pred-name] (str/split (get % "_predicate/name") #"/")]
                              (assoc % :collection (keyword collection-name) :predicate (keyword pred-name))))
                     (mapv (fn [a] (->> a (mapv #(vector (keyword (key %)) (val %))) (into {}))))
                     (group-by :collection))
            fuel   (if fuel @fuel 0)]
        [fuel (add-reverse-joins schema)]))))

(def schema-cache (atom (cache/ttl-cache-factory {} :ttl (* 10 60 1000))))

(defn- schema-lookup
  [db dbid opts]
  (go-try
    (if-let [cached-schema (get @schema-cache dbid)]
      [0 cached-schema]
      (let [[fuel schema] (<? (pull-schema db opts))]
        (swap! schema-cache assoc dbid schema)
        [fuel schema]))))


(defn format-schema
  [db opts]
  (go-try
    (let [[fuel schema] (<? (schema-lookup db (:dbid db) opts))
          schema* (into [] (concat [input-object-keys]
                                   (mapv format-types base-types-formatted)
                                   [flake]
                                   [(graph-schema (into [] (keys schema)))]
                                   [mutationRoot]
                                   [queryRoot]
                                   [sortOrder]
                                   (mapv collection-types schema)))]
      [fuel schema*])))


(defn trim-all
  [str]
  (-> str
      (str/replace #"\s+" "")
      (str/replace #"\r+" "")
      (str/replace #"\n+" "")
      (str/replace #"('|\")" "")))

(defn trim-bracket-l
  [str]
  (if (str/ends-with? str "]")
    (subs str 0 (- (count str) 1))))


(defn translate-nested-options
  [keyword]
  (let [dictionary {:where     :where
                    :limit     :_limit
                    :recur     :_recur
                    :as        :_as
                    :component :_component
                    :sort      :_orderBy}
        keyword*   (get dictionary keyword keyword)]
    keyword*))

(defn try-coerce->int
  [string]
  (let [res (try*
              (read-string string)
              (catch* e (str (str string))))]
    (if (= clojure.lang.Symbol (type res))
      (str res)
      res)))

(defn parse-options
  ([option-map]
   (parse-options option-map true))
  ([option-map top-level]
   (reduce-kv (fn [acc opt-key opt-val]
                (let [opt-key' (cond
                                 (and top-level (= :sort (keyword opt-key)))
                                 :orderBy

                                 top-level
                                 (keyword opt-key)

                                 :else
                                 (translate-nested-options (keyword opt-key)))
                      opt-val' (cond (#{:limit :_limit} opt-key)
                                     (read-string opt-key)

                                     (= :block opt-key)
                                     (try-coerce->int opt-key)

                                     (#{:orderBy :_orderBy} opt-key')
                                     [(str (nth opt-val 3)) (str (nth opt-val 1))]

                                     :else
                                     opt-val)]
                  (assoc acc opt-key' opt-val'))) {} option-map)))


(defn select-parse
  [select-array]
  (let [select-array* (mapv (fn [select]
                              (cond
                                (string? select)
                                select

                                (symbol? select)
                                (str select)

                                (map? select)
                                (parse-options select false)

                                (vector? select)
                                (if (every? #(not (vector? %)) select)
                                  (mapv (fn [n]
                                          (if (map? n)
                                            n (str n)))
                                        select)
                                  (select-parse select))

                                :else
                                select)) select-array)
        last-index    (- (count select-array*) 1)
        final-select  (reduce-kv (fn [acc key val]
                                   (cond
                                     (and (string? val) (= last-index key))
                                     (into acc [val])

                                     (string? val)
                                     (let [next     (nth select-array* (+ 1 key))
                                           options  (if (map? next)
                                                      next nil)
                                           nested   (cond
                                                      options
                                                      (nth select-array* (+ 2 key))

                                                      (vector? next)
                                                      next

                                                      :else
                                                      nil)
                                           combined (into [] (remove nil? (cons options nested)))
                                           select   (if (nil? nested)
                                                      val
                                                      {(keyword val) combined})]
                                       (into acc [select]))

                                     :else
                                     acc)) [] select-array*)] final-select))

(defn retrieve-schema-type
  [schema-item type-select]
  (reduce
    (fn [acc select]
      (let [[select' item] (cond
                             (or (symbol? select) (string? select))
                             [(keyword (str select)) (get schema-item (keyword (str select)))]

                             (and (map? select) (vector? (first (vals select))))
                             (let [from            (first (keys select))
                                   sub-schema-item (get schema-item (keyword from))
                                   select'         (get select from)
                                   item'           (if (vector? sub-schema-item)
                                                     (map #(retrieve-schema-type % select') sub-schema-item)
                                                     (retrieve-schema-type sub-schema-item select'))]
                               [(keyword from) item'])

                             :else
                             [nil nil])]
        (if (not (nil? select'))
          (assoc acc select' item)
          acc)))
    {} type-select))


(defn parse-introspection-query
  [db intro-query opts]
  (go-try
    (let [intro-keys  (keys intro-query)
          [fuel schema] (<? (format-schema db opts))
          type-select (get-in intro-query [:types :select])
          res         (mapv (fn [k]
                              (cond
                                (= :queryType k)
                                {:queryType {:name "QueryRoot"}}

                                (= :directives k)
                                {:directives []}

                                (= :mutationType k)
                                {:mutationType {:name "MutationRoot"}}

                                (= :subscriptionType k)
                                {:subscriptionType nil}

                                (= :types k)
                                {:types (mapv #(retrieve-schema-type % type-select) schema)}))
                            intro-keys)
          res'        {:result {:__schema (reduce (fn [acc m]
                                                    (merge acc m)) {} res)}
                       :type   :__schema
                       :status 200}]
      (if (:meta opts)
        (assoc res' :fuel fuel)
        res'))))

(defn parse-generic-query
  [query-str]
  (let [rest-vec* (read-string query-str)
        query-map (reduce-kv (fn [acc key elem]
                               (if (not (coll? elem))
                                 (let [collection    (str elem)
                                       next-elem     (nth rest-vec* (+ 1 key))
                                       options       (if (map? next-elem)
                                                       (parse-options next-elem)
                                                       nil)
                                       value         (if (map? next-elem)
                                                       (nth rest-vec* (+ 2 key))
                                                       next-elem)
                                       _             (if-not (vector? value)
                                                       (throw (ex-info (str "Invalid query structure. Cannot select predicates from: " value)
                                                                       {:status 400
                                                                        :error  :db/invalid-query})))
                                       parsed-select (select-parse value)
                                       from          (or (:_id options) (:ident options) collection)
                                       query         (merge {:select parsed-select
                                                             :from   from}
                                                            options)
                                       full-query    {(keyword collection) query}]
                                   (merge acc full-query)) acc))
                             {} rest-vec*)]
    query-map))


(defn read-and-add-fragments
  [graphql-str]
  "Takes a GraphQL string, replaces all fragments"
  (let [vec*             (try*
                           (read-string graphql-str)
                           (catch* ex
                                   (read-string (str graphql-str "]"))))
        vec**            (mapv (fn [n]
                                 (if (symbol? n)
                                   (str/trim (str n))
                                   n))
                               vec*)
        fragment-indices (remove nil? (map-indexed
                                        (fn [idx item]
                                          (if (= item "fragment") idx))
                                        vec**))
        de-fragged       (if-not (empty? fragment-indices) (loop [vec-str (str vec**)
                                                                  vec     vec**
                                                                  f-idxs  fragment-indices]
                                                             (let [f-name   (str/trim (str (nth vec** (+ (first f-idxs) 1))))
                                                                   _        (if-not (= "on" (str/trim (str (nth vec** (+ (first f-idxs) 2)))))
                                                                              (throw (ex-info (str "Invalid fragment structure: " (nth vec** (+ (first f-idxs) 4)))
                                                                                              {:status 400
                                                                                               :error  :db/invalid-query})))
                                                                   fragment (-> (nth vec** (+ (first f-idxs) 4))
                                                                                str
                                                                                (str/replace-first #"\[" "")
                                                                                str/trim-newline
                                                                                str/trim
                                                                                str/trim-newline
                                                                                str/trim
                                                                                trim-bracket-l)
                                                                   new-str  (str/replace vec-str (str "..." f-name) fragment)]
                                                               (if (empty? (rest f-idxs))
                                                                 new-str
                                                                 (recur new-str vec (rest f-idxs))))))
        de-fragged*      (if-not (empty? fragment-indices) (read-string de-fragged))
        remove-indices   (if-not (empty? fragment-indices) (reduce (fn [acc n]
                                                                     (into acc (range n (+ 5 n))))
                                                                   [] fragment-indices))
        query-vec        (if-not (empty? fragment-indices) (into [] (remove nil? (map-indexed
                                                                                   (fn [idx v]
                                                                                     (if-not ((into #{} remove-indices) idx)
                                                                                       v))
                                                                                   de-fragged*))))
        query            (cond
                           (empty? fragment-indices)
                           (str (first (first vec*)))

                           (and (= 1 (count (first query-vec))) (= 1 (count query-vec)))
                           (str (first (first query-vec)))

                           (= 1 (count query-vec))
                           (str (first query-vec))

                           :else
                           (str query-vec))]
    query))

(defn process-block-query
  [query-str]
  (let [cleaned-str (-> query-str
                        trim-all
                        (str/replace "[" "{")
                        (str/replace "]" "}")
                        (str/replace "block" " :block ")
                        (str/replace "from" " :from ")
                        (str/replace "to" " :to ")
                        read-string)
        _           (if (empty? (:block cleaned-str))
                      (throw (ex-info (str "Block query not properly formatted" query-str)
                                      {:status 400
                                       :error  :db/invalid-query})))
        from        (get-in cleaned-str [:block :from])
        _           (if (nil? from)
                      (throw (ex-info (str "Block queries require a from block" query-str)
                                      {:status 400
                                       :error  :db/invalid-query})))
        to          (get-in cleaned-str [:block :to])
        final-query {:type  :block
                     :block [from to]}]
    final-query))

(defn parse-mutation
  [body]
  (let [parsed     (read-string body)
        parsed-key (-> parsed
                       second
                       keys
                       first
                       str)
        _          (if-not (= "tx" parsed-key)
                     (throw (ex-info (str "Transaction improperly formatted: " body)
                                     {:status 400
                                      :error  :db/invalid-tx})))
        tx-body    (first (vals (second parsed)))
        tx         (mapv #(apply assoc {} %) tx-body)]
    {:tx tx}))


(defn add-variables
  [query-str variables]
  (let [query-with-vars (reduce
                          (fn [acc var]
                            (let [new-str (str/replace acc
                                                       (re-pattern (str "\\" "$" (util/keyword->str (key var))))
                                                       (val var))]
                              new-str))
                          query-str variables)]
    query-with-vars))


(defn parse-type-query
  [db query opts]
  (go-try
    (if-not (or {:__type query}
                (contains? (:__type query) :name)
                (contains? (:__type query) :from)
                (contains? (:__type query) :select))
      (ex-info (str "Invalid type query " (pr-str query))
               {:status 400
                :error  :db/invalid-query})
      (let [type-name  (get-in query [:__type :name])
            schema-res (<? (format-schema db opts))]
        (if (util/exception? schema-res)
          schema-res
          (let [[fuel schema] schema-res
                schema-item (filterv #(= type-name (:name %))
                                     schema)
                select      (get-in query [:__type :select])
                res         {:type   :__type
                             :result (retrieve-schema-type (first schema-item) select)
                             :status 200}]
            (if (:meta opts)
              (assoc res :fuel fuel)
              res)))))))


(defn parse-graph-opts
  [graphql-str]
  (let [opt-str  (second (str/split graphql-str #"\{\s*"))
        opt-map  (-> (str "{ " opt-str " }")
                     (str/replace #"\(" " { ")
                     (str/replace #"\)" " } ")
                     (str/replace #":" " ")
                     (str/replace #"," " "))
        opt-map' (try*
                   (-> opt-map
                       read-string
                       vals
                       first)
                   (catch* ex nil))
        opts     (reduce-kv (fn [acc k v]
                              (let [k* (cond (symbol? k)
                                             (keyword k)

                                             (string? k)
                                             (util/keyword->str k)

                                             :else
                                             k)
                                    v* (if (symbol? v) (str v) v)]
                                (assoc acc k* v*))) {} opt-map')]
    opts))

(defn parse-history-query
  [query-str]
  (let [query-str' (-> (str/replace query-str #"block" ":block")
                       (str/replace #"prettyPrint" ":prettyPrint")
                       (str/replace #"pretty-print" ":prettyPrint")
                       (str/replace #"subject" ":subject"))
        query      (read-string query-str')
        history?   (= "history" (trim-all (first query)))
        _          (if-not history?
                     (throw (ex-info (str "History query not properly formatted. Provided: " query)
                                     {:status 400
                                      :error  :db/invalid-query})))
        query-map  (second query)
        subject    (:subject query-map)
        _          (when (nil? subject)
                     (throw (ex-info (str "History query not properly formatted - missing subject. Provided: " query)
                                     {:status 400
                                      :error  :db/invalid-query})))]
    {:type        :history
     :block       (:block query-map)
     :prettyPrint (or (:pretty-print query-map) (:prettyPrint query-map))
     :history     (read-string subject)}))

(defn clean-where-and-block-query
  [query]
  "FlureeQL does not allow a query with both where and from clauses. If a where is included at the top-level
  of a GraphQL query, we dissoc :from
  Additionally, query+ only considers :block declared at the top-level."
  (let [from-where-clean (reduce-kv (fn [acc key value]
                                      (let [new-acc   (if (and (contains? value :from)
                                                               (contains? value :where))
                                                        (assoc acc key (dissoc value :from))
                                                        (assoc acc key value))
                                            block-vec (concat (:block acc) [(:block value)])]
                                        (assoc new-acc :block block-vec))) {} query)
        block-vec        (remove nil? (:block from-where-clean))
        _                (if (some #(not= (first block-vec) %) block-vec)
                           (throw (ex-info (str "When submitting multiple queries at once, the specified block must be the same.")
                                           {:status 400
                                            :error  :db/invalid-query})))]
    (if (empty? block-vec)
      (dissoc from-where-clean :block)
      (assoc from-where-clean :block (first block-vec)))))

(defn gql-query-type
  [graphql-str]
  (let [split-str (str/split graphql-str #"\{\s*")
        [first-str second-str] [(trim-all (first split-str)) (trim-all (second split-str))]
        type      (cond
                    (boolean (re-find #"mutation" first-str))
                    :mutation

                    (boolean (re-find #"__type" second-str))
                    :type

                    (boolean (re-find #"history" second-str))
                    :history

                    (and (boolean (re-find #"graph" second-str)) (boolean (re-find #"block" second-str)))
                    :graph-with-block

                    (boolean (re-find #"graph" second-str))
                    :graph

                    (boolean (re-find #"block" second-str))
                    :block

                    (boolean (re-find #"__schema" second-str))
                    :intro

                    :else
                    (throw (ex-info (str "Unrecognized GraphQL query type. Provided: " (pr-str graphql-str))
                                    {:status 400
                                     :error  :db/invalid-query})))]
    type))

(defn replace-gql-chars
  [query-with-vars]
  (str "[ [ " (-> query-with-vars
                  (str/replace #"_Via" "/")
                  (str/replace #"\{" "[")
                  (str/replace #"\}" "]")
                  (str/replace #"\(" " {")
                  (str/replace #"\)" "} ")
                  (str/replace #"," " ")
                  (str/replace #":" " ")) "]"))

(defn gql-query-str->array
  [graphql-str type vars]
  (let [split-str        (str/split graphql-str #"\{\s*")
        parse-str        (if (#{:graph :intro} type)
                           (str "{" (str/join "{" (drop 2 split-str)))
                           (str "{" (str/join "{" (drop 1 split-str))))
        query-with-vars  (add-variables parse-str vars)
        query-char-rep   (replace-gql-chars query-with-vars)
        query-with-frags (read-and-add-fragments query-char-rep)]
    query-with-frags))

(defn get-block-for-graph-query
  [query-array]
  (let [block (second query-array)]
    (first (vals block))))

(defn parse-graphql-to-flureeql
  "Takes a GraphQL str, and parses to FlureeQL query syntax"
  ([db graphql-str]
   (parse-graphql-to-flureeql db graphql-str nil {}))
  ([db graphql-str variables]
   (parse-graphql-to-flureeql db graphql-str variables {}))
  ([db graphql-str variables opts]
   (go-try
     (let [type             (gql-query-type graphql-str)
           query-array      (gql-query-str->array graphql-str type variables)
           graph-query-opts (if (= type :graph)
                              (parse-graph-opts graphql-str))
           query-map        (condp = type
                              :block
                              (process-block-query query-array)

                              :intro
                              (<?
                                (parse-introspection-query db (parse-generic-query query-array) opts))

                              :mutation
                              (parse-mutation query-array)

                              :type
                              (<?
                                (parse-type-query db (parse-generic-query query-array) opts))

                              :history
                              (parse-history-query query-array)

                              :graph-with-block
                              (let [query-arr  (read-string query-array)
                                    block      (get-block-for-graph-query query-arr)
                                    query-arr' (nth query-arr 2)
                                    query      (parse-generic-query (str query-arr'))]
                                (assoc query :block block))

                              :graph
                              (let [query  (parse-generic-query query-array)
                                    query' (clean-where-and-block-query query)]
                                (merge graph-query-opts query')))]
       query-map))))
