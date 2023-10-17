(ns fluree.db.query.fql.syntax
  (:require [clojure.string :as str]
            [fluree.db.util.core :refer [try* catch*]]
            [fluree.db.util.log :as log]
            [fluree.db.validation :as v]
            [fluree.db.util.docs :as docs]
            [malli.core :as m]
            [malli.error :as me]
            [malli.transform :as mt]))

#?(:clj (set! *warn-on-reflection* true))
(declare registry)

(defn wildcard?
  [x]
  (#{"*" :* '*} x))

(defn asc?
  [x]
  (boolean (#{'asc "asc" :asc} x)))

(defn desc?
  [x]
  (boolean (#{'desc "desc" :desc} x)))

(defn one-select-key-present?
  [q]
  (log/trace "one-select-key-present? q:" q)
  (if (map? q)
    (let [skeys (->> q keys
                     (map #{:select :selectOne :select-one :selectDistinct
                            :select-distinct})
                     (remove nil?))]
      (log/trace "one-select-key-present? skeys:" skeys)
      (= 1 (count skeys)))
    true))

(defn sequential-where?
  [q]
  (if (map? q)
    (let [{:keys [where]} q]
      (log/trace "sequential-where? where:" where)
      (if-not (sequential? where)
        where
        true))
    true))

(defn wrapped-where?
  [q]
  (log/trace "wrapped-where? q:" q)
  (if (map? q)
    (let [{:keys [where]} q
          invalid-clauses (remove #(or (sequential? %)
                                       (map? %)) where)]
      (log/warn "wrapped-where? invalid clauses: " invalid-clauses)
      (empty? invalid-clauses))
    true))

(defn valid-selector-types?
  [q]
  (log/trace "valid-selector-types? q:" q)
  (if (map? q)
    (let [{:keys [select]} q]
      (every? #(or (map? %)
                   (v/variable? %)) select))
    true))


(defn valid-where-map-sizes?
  [q]
  (log/trace "valid-where-map-sizes? q:" q)
  (if (map? q)
    (let [{:keys [where]} q]
      (if-let [where-maps (filter map? where)]
        (every? #(= 1 (count %)) where-maps)
        true))
    true))

(defn valid-where-ops?
  [q]
  (log/trace "valid-where-ops? q: " q)
  (if (map? q)
    (let [{:keys [where]} q]
      (if-let [where-ops (not-empty (mapcat keys (filter map? where)))]
        (every? #(m/validate (m/schema ::v/where-op {:registry registry}) % ) where-ops)
        true))
    true))

(defn valid-group-by?
  [q]
  (log/trace "valid-group-by? q:" q)
  (if (map? q)
    (if-let [group-by (-> (select-keys q [:group-by :groupBy])
                          not-empty)]
      (let [group-by (-> group-by vals first)]
        (or (v/variable? group-by)
            (and (sequential? group-by)
                 (every? v/variable? group-by))))
      true)
    true))

(defn valid-order-by?
  [q]
  (log/trace "valid-order-by? q:" q)
  (if (map? q)
    (if-let [order-by (-> q
                       (select-keys [:order-by :orderBy])
                       not-empty)]
      (let [order-by (-> order-by vals first)]
        (or (v/variable? order-by)
            (and (sequential? group-by)
                 (= 2 (count group-by))
                 (or (asc? (first group-by))
                     (desc? (first group-by))))))
      true)
    true))

(defn valid-bind?
  [q]
  (log/trace "valid-bind? q:" q)
  (if (map? q)
    (if-let [[_ bind] (find q :bind)]
      (m/validate (m/schema ::v/bind {:registry registry}) bind)
      true)
    true))

(defn valid-filter?
  (log/trace "valid-filter? q:" q)
  (if (map? q)
    (if-let [[_ filter] (find q :filter)]
      (m/validate (m/schema ::v/filter {:registry registry}) filter)
      true)
    true))


(def error-message-schema
  [:and
   [:map-of ::json-ld-keyword :any]
   [:fn
    {:error/fn
     (fn [{:keys [value]} _]
       (str "Query: " (pr-str value) " contains an invalid where clause."
            "Where clause must be a vector/array of tuples and/or maps."))}
    sequential-where?]
   [:fn
    {:error/fn
     (fn [{:keys [value]} _]
       (str "Query: " (pr-str value) " contains an invalid where pattern. "
            "Every pattern must be a tuple or map "))}
    wrapped-where?]
   [:fn
    {:error/fn
     (fn [{:keys [value]} _]
       (str "Query: " (pr-str value) " contains an invalid select statement. "
            "Every selection must be a variable or map."))}
    valid-selector-types?]
   [:fn
    {:error/fn
     (fn [{:keys [value]} _]
       (str "Query: " (pr-str value) " contains an invalid where clause. "
            "Maps in where clause can only have one key/val pair."))}
    valid-where-map-sizes?]
   [:fn
    {:error/fn
     (fn [{:keys [value] :as e} _]
       (let [unrecognized-ops (->> value
                                   :where
                                   (filter map?)
                                   (mapcat keys)
                                   (remove #{:filter :optional :union :bind}))]
         (str "Unrecognized operation in where map: " (str/join ", " unrecognized-ops))))}
    valid-where-ops?]
   [:fn
    {:error/fn
     (fn [{:keys [value] :as e} _]
       (let [group-by (select-keys value [:group-by :groupBy])]
         (str "Invalid group-by, must be a variable or vector of variables, provided: " group-by )))}
    valid-group-by?]
   [:fn
    {:error/fn
     (fn [{:keys [value] :as e} _]
       (str "Query " value " contains an invalid orderBy clause, must be variable or two-tuple formatted ['ASC' or 'DESC', var]"))}
    valid-order-by?]
   [:fn
    {:error/fn
     (fn [{:keys [value] :as e} _]
       (str "Query " value " contains an invalid bind clause, TODO"))}
    valid-bind?]
   [:fn
    {:error/fn
     (fn [{:keys [value] :as e} _]
       (str "Query " value " contains an invalid filter clause, TODO"))}
    valid-filter??]])

(defn query-schema
  "Returns schema for queries, with any extra key/value pairs `extra-kvs`
  added to the query map.
  This allows eg http-api-gateway to amend the schema with required key/value pairs
  it wants to require, which are not required/supported here in the db library."
  [extra-kvs]
  [:and
   [:map-of ::json-ld-keyword :any]
   [:fn
    {:error/fn
     (fn [{:keys [value]} _]
       (str "Query: " (pr-str value) " does not have exactly one select clause. "
            "One of 'select', 'selectOne', 'select-one', 'selectDistinct', or 'select-distinct' is required in queries. "
            "See documentation here for more details: "
            docs/error-codes-page "#query-missing-select"))}
    one-select-key-present?]
      (into [:map {:closed true}
          [:where ::where]
          [:t {:optional true} ::t]
          [:context {:optional true} ::context]
          [:select {:optional true} ::select]
          [:selectOne {:optional true} ::select]
          [:select-one {:optional true} ::select]
          [:selectDistinct {:optional true} ::select]
          [:select-distinct {:optional true} ::select]
          [:orderBy {:optional true} ::order-by]
          [:order-by {:optional true} ::order-by]
          [:groupBy {:optional true} ::group-by]
          [:group-by {:optional true} ::group-by]
          [:filter {:optional true} ::filter]
          [:having {:optional true} ::function]
          [:values {:optional true} ::values]
          [:limit {:optional true} ::limit]
          [:offset {:optional true} ::offset]
          [:maxFuel {:optional true} ::max-fuel]
          [:max-fuel {:optional true} ::max-fuel]
          [:depth {:optional true} ::depth]
          [:opts {:optional true} ::opts]
          [:prettyPrint {:optional true} ::pretty-print]
          [:pretty-print {:optional true} ::pretty-print]]
         extra-kvs)])

(def registry
  (merge
   (m/predicate-schemas)
   (m/class-schemas)
   (m/comparator-schemas)
   (m/type-schemas)
   (m/sequence-schemas)
   (m/base-schemas)
   v/registry
   {::limit           pos-int?
    ::offset          nat-int?
    ::max-fuel        pos-int?
    ::depth           nat-int?
    ::pretty-print    boolean?
    ::parse-json      boolean?
    ::context-type    [:enum :string :keyword]
    ::issuer          [:maybe string?]
    ::role            :any
    ::did             :any
    ::opts            [:and
                       [:map-of :keyword :any]
                       [:map
                        [:maxFuel {:optional true} ::max-fuel]
                        [:max-fuel {:optional true} ::max-fuel]
                        [:parseJSON {:optional true} ::parse-json]
                        [:parse-json {:optional true} ::parse-json]
                        [:prettyPrint {:optional true} ::pretty-print]
                        [:pretty-print {:optional true} ::pretty-print]
                        [:contextType {:optional true} ::context-type]
                        [:context-type {:optional true} ::context-type]
                        [:issuer {:optional true} ::issuer]
                        [:role {:optional true} ::role]
                        [:did {:optional true} ::did]]]
    ::function        ::v/function
    ::wildcard        [:fn wildcard?]
    ::var             ::v/var
    ::iri             ::v/iri
    ::subject         ::v/subject
    ::subselect-map   [:map-of {:error/message "Must be map from iri to subselection"}
                       ::iri [:ref ::subselection]]
    ::subselection    [:sequential {:error/message
                                    "Invalid subselection"}
                       [:orn
                        [:wildcard ::wildcard]
                        [:predicate ::iri]
                        [:subselect-map [:ref ::subselect-map]]]]
    ::select-map      [:map-of {:max 1
                                :error/message "Only one key/val for select-map"}
                       ::var ::subselection]
    ::selector        [:orn {:error/message "Must be either a variable, iri, function application, or select map"}
                       [:var ::var]
                       [:pred ::iri]
                       [:aggregate ::function]
                       [:select-map ::select-map]]
    ::select          [:orn {:error/message "Invalid select statement"}
                       [:selector ::selector]
                       [:collection [:sequential ::selector]]]
    ::direction       [:orn {:error/fn (fn [{:keys [value]} _]
                                         (str "Unknown ordering: " (pr-str value)))}
                       [:asc [:fn asc?]]
                       [:desc [:fn desc?]]]
    ::ordering        [:orn {:error/messge "Must be a variable or two-tuple formatted ['ASC' or 'DESC', var]"}
                       [:scalar ::var]
                       [:vector [:and list?
                                 [:catn
                                  [:direction ::direction]
                                  [:dimension ::var]]]]]
    ::order-by        [:orn {:error/message  "Invalid orderBy clause"}
                       [:clause ::ordering]
                       [:collection [:sequential ::ordering]]]
    ::group-by          [:orn #_{:error/message "Invalid groupBy clause, must be a variable or a vector of variables."}
                         [:clause ::var]
                         [:collection [:sequential ::var]]]
    ::triple          ::v/triple
    ::filter          ::v/filter
    ::where           ::v/where
    ::values          ::v/values
    ::t               [:or :int :string]
    ::context         ::v/context
    ::json-ld-keyword ::v/json-ld-keyword
    ::query           (query-schema [])
    ::modification    ::v/modification-txn}))

(def triple-validator
  (m/validator ::triple {:registry registry}))

(defn triple?
  [x]
  (triple-validator x))

(def coerce-query*
  (m/coercer ::query (mt/transformer {:name :fql}) {:registry registry}))

(def default-error-overrides
  {:errors
   (-> me/default-errors
       (assoc
         ::m/missing-key
         {:error/fn
          (fn [{:keys [in]} _]
            (let [k (-> in last name)]
              (str "Query is missing a '" k "' clause. "
                   "'" k "' is required in queries. "
                   "See documentation here for details: "
                   docs/error-codes-page "#query-missing-" k)))}
         ::m/extra-key
         {:error/fn
          (fn [{:keys [in]} _]
            (let [k (-> in last name)]
              (str "Query contains an unknown key: '" k "'. "
                   "See documentation here for more information on allowed query keys: "
                   docs/error-codes-page "#query-unknown-key")))}))})

(defn humanize-error
  [error]
  (let [explain-data (-> error ex-data :data :explain)]
    (log/trace "query validation error:"
               (update explain-data :errors
                       (fn [errors] (map #(dissoc % :schema) errors))))
    (-> explain-data
        (v/format-explained-error default-error-overrides))))


(defn coerce-query
  [qry]
  (try*
    (coerce-query* qry)
    (catch* e
            (let [error-msg        (humanize-error e)
                  _         (log/trace "humanized errors:" error-msg)]
              (throw (ex-info error-msg {:status 400, :error :db/invalid-query}))))))

(def coerce-modification*
  (m/coercer ::modification (mt/transformer {:name :fql}) {:registry registry}))

(defn coerce-modification
  [mdfn]
  (try*
   (coerce-modification* mdfn)
   (catch* e
     (throw (ex-info "Invalid Ledger Modification"
                     {:status  400
                      :error   :db/invalid-query
                      :reasons (humanize-error e)})))))
