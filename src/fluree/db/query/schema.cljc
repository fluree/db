(ns fluree.db.query.schema
  (:require [fluree.db.flake :as flake #?@(:cljs [:refer [Flake]])]
            [fluree.db.constants :as const]
            [fluree.db.util.async :refer [<? go-try into?]]
            [fluree.db.query.range :as query-range]
            [fluree.db.util.core :as util :refer [try* catch*]]
            [fluree.db.util.iri :as iri-util])
  #?(:clj (:import (fluree.db.flake Flake))))

;; map of tag subject ids for each of the _predicate/type values for quick lookups
(def ^:const type-sid->type {(flake/->sid const/$_tag const/_predicate$type:string)   :string
                             (flake/->sid const/$_tag const/_predicate$type:ref)      :ref
                             (flake/->sid const/$_tag const/_predicate$type:boolean)  :boolean
                             (flake/->sid const/$_tag const/_predicate$type:instant)  :instant
                             (flake/->sid const/$_tag const/_predicate$type:uuid)     :uuid
                             (flake/->sid const/$_tag const/_predicate$type:uri)      :uri
                             (flake/->sid const/$_tag const/_predicate$type:bytes)    :bytes
                             (flake/->sid const/$_tag const/_predicate$type:int)      :int
                             (flake/->sid const/$_tag const/_predicate$type:long)     :long
                             (flake/->sid const/$_tag const/_predicate$type:bigint)   :bigint
                             (flake/->sid const/$_tag const/_predicate$type:float)    :float
                             (flake/->sid const/$_tag const/_predicate$type:double)   :double
                             (flake/->sid const/$_tag const/_predicate$type:bigdec)   :bigdec
                             (flake/->sid const/$_tag const/_predicate$type:tag)      :tag
                             (flake/->sid const/$_tag const/_predicate$type:json)     :json
                             (flake/->sid const/$_tag const/_predicate$type:geojson)  :geojson
                             (flake/->sid const/$_tag const/_predicate$type:date)     :date
                             (flake/->sid const/$_tag const/_predicate$type:time)     :time
                             (flake/->sid const/$_tag const/_predicate$type:dateTime) :dateTime
                             (flake/->sid const/$_tag const/_predicate$type:duration) :duration})

(def ^:const lang-sid->lang {(flake/->sid const/$_tag const/_setting$language:ar) :ar
                             (flake/->sid const/$_tag const/_setting$language:bn) :bn
                             (flake/->sid const/$_tag const/_setting$language:br) :br
                             (flake/->sid const/$_tag const/_setting$language:cn) :cn
                             (flake/->sid const/$_tag const/_setting$language:en) :en
                             (flake/->sid const/$_tag const/_setting$language:es) :es
                             (flake/->sid const/$_tag const/_setting$language:fr) :fr
                             (flake/->sid const/$_tag const/_setting$language:hi) :hi
                             (flake/->sid const/$_tag const/_setting$language:id) :id
                             (flake/->sid const/$_tag const/_setting$language:ru) :ru})

(defn flake->pred-map
  [flakes]
  (reduce (fn [acc ^Flake flake]                                   ;; quick lookup map of predicate's predicate ids
            (let [p         (.-p flake)
                  o         (.-o flake)
                  existing? (get acc p)]
              (cond (and existing? (vector? existing?))
                    (update acc p conj o)

                    existing?
                    (update acc p #(vec [%1 %2]) o)

                    :else
                    (assoc acc p o))))
          {} flakes))

(defn- extract-spec-ids
  [spec-pid schema-flakes]
  (->> schema-flakes
       (keep #(let [f ^Flake %]
                (when (= spec-pid (.-p f)) (.-o f))))
       vec))

(defn- recur-sub-classes
  "Once an initial parent->child relationship is established, recursively place
  children into parents to return a sorted set of all sub-classes regardless of depth
  Sorted set is used to ensure consistent query results.

  First takes predicate items and makes a map like this of parent -> children:
  {100 [200 201]
   201 [300 301]}

  Then recursively gets children's children to return a map like this:
  {100 #{200 201 300 301}
   201 #{300 301}}

   Initial pred-items argument looks like:
   #{{:iri 'http://schema.org/Patient', :class true, :subclassOf [1002], :id 1003} ...}
   "
  [pred-items]
  (let [subclass-map (reduce
                       (fn [acc class]
                         (if-let [parent-classes (:subclassOf class)]
                           (reduce #(update %1 %2 conj (:id class)) acc parent-classes)
                           acc))
                       {} pred-items)]
    (reduce-kv
      (fn [acc parent children]
        (loop [[child & r] children
               all-children (apply sorted-set children)]
          (if (nil? child)
            (assoc acc parent all-children)
            (if-let [child-children (get subclass-map child)]
              (recur (into child-children r) (into all-children child-children))
              (recur r all-children)))))
      {} subclass-map)))


(defn- pred-map->classes
  "Filters the predicate map to only include classes"
  [pred-map]
  ;; in the predicate map, classes are duplicated with both subject id (number
  ;; and iri - so here we just keep ones with subject-id so there are no duplicates
  (keep #(when (and (number? (key %)) (:class (val %)))
           (val %))
        pred-map))


(defn calc-subclass
  "Calculates subclass map for use with queries for rdf:type."
  [predicate-map]
  (let [classes      (pred-map->classes predicate-map)
        subclass-map (recur-sub-classes classes)]
    ;; map subclasses for both subject-id and iri
    (reduce
      (fn [acc class]
        (assoc acc (:id class) (get subclass-map (:id class))
                   (:iri class) (get subclass-map (:id class))))
      {} classes)))


(defn schema-map
  "Returns a map of the schema for a db to allow quick lookups of schema properties.
  Schema is a map with keys:
  - :t - the 't' value when schema built, allows schema equality checks
  - :coll - collection info, mapping cid->name and name->cid all within the same map
  - :pred - predicate info, mapping pid->properties and name->properties for quick lookup based on id or name respectively
  - :fullText - contains predicate ids that need fulltext search
  "
  [db]
  (go-try
    (let [schema-flakes    (<? (query-range/index-range db :spot
                                                        >= [(flake/max-subject-id const/$_collection)]
                                                        <= [0]))
          ;; retrieve prefix flakes in background, process last
          prefix-flakes-ch (query-range/index-range db :spot
                                                    >= [(flake/max-subject-id const/$_prefix)]
                                                    <= [(flake/min-subject-id const/$_prefix)])
          [collection-flakes predicate-flakes] (partition-by #(<= (.-s ^Flake %) flake/MAX-COLL-SUBJECTS) schema-flakes)
          coll             (->> collection-flakes
                                (partition-by #(.-s ^Flake %))
                                (reduce (fn [acc coll-flakes]
                                          (let [^Flake first-flake (first coll-flakes)
                                                sid       (.-s first-flake)
                                                p->v      (->> coll-flakes ;; quick lookup map of collection's predicate ids
                                                               (reduce #(let [f ^Flake %2]
                                                                          (assoc %1 (.-p f) (.-o f)))
                                                                       {}))
                                                partition (or (get p->v const/$_collection:partition)
                                                              (flake/sid->i sid))
                                                c-name    (get p->v const/$_collection:name)
                                                specs     (when (get p->v const/$_collection:spec) ;; specs are multi-cardinality - if one exists filter through to get all
                                                            (extract-spec-ids const/$_collection:spec coll-flakes))
                                                specDoc   (get p->v const/$_collection:specDoc)
                                                c-props   {:name      c-name
                                                           :sid       sid
                                                           :spec      specs
                                                           :specDoc   specDoc
                                                           :id        partition ;; TODO - deprecate! (use partition instead)
                                                           :partition partition
                                                           :base-iri  (get p->v const/$_collection:baseIRI)}]
                                            (assoc acc partition c-props
                                                       c-name c-props)))
                                        ;; put in defaults for _tx
                                        {-1    {:name "_tx" :id -1 :sid -1}
                                         "_tx" {:name "_tx" :id -1 :sid -1}}))
          [pred fullText] (->> predicate-flakes
                               (partition-by #(.-s ^Flake %))
                               (reduce (fn [[pred fullText] pred-flakes]
                                         (let [^Flake first-flake (first pred-flakes)
                                               id        (.-s first-flake)
                                               p->v      (flake->pred-map pred-flakes)
                                               class?    (contains? p->v const/$rdf:type)
                                               iri       (get p->v const/$iri)
                                               equivs    (when-let [equivs (get p->v const/$_predicate:equivalentProperty)]
                                                           (if (sequential? equivs) equivs [equivs]))
                                               p-name    (get p->v const/$_predicate:name)
                                               p-type    (->> (get p->v const/$_predicate:type)
                                                              (get type-sid->type))
                                               ref?      (boolean (#{:ref :tag} p-type))
                                               idx?      (boolean (or ref?
                                                                      (get p->v const/$_predicate:index)
                                                                      (get p->v const/$_predicate:unique)))
                                               fullText? (get p->v const/$_predicate:fullText)
                                               p-props   (if class?
                                                           {:iri        iri
                                                            :class      true
                                                            :subclassOf (when-let [sc (get p->v const/$rdfs:subClassOf)]
                                                                          (if (sequential? sc) sc [sc]))
                                                            :id         id}
                                                           {:name               p-name
                                                            :id                 id
                                                            :iri                iri
                                                            :equivalentProperty equivs
                                                            :type               p-type
                                                            :ref?               ref?
                                                            :idx?               idx?
                                                            :unique             (boolean (get p->v const/$_predicate:unique))
                                                            :multi              (boolean (get p->v const/$_predicate:multi))
                                                            :index              (boolean (get p->v const/$_predicate:index))
                                                            :upsert             (boolean (get p->v const/$_predicate:upsert))
                                                            :component          (boolean (get p->v const/$_predicate:component))
                                                            :noHistory          (boolean (get p->v const/$_predicate:noHistory))
                                                            :restrictCollection (get p->v const/$_predicate:restrictCollection)
                                                            :retractDuplicates  (boolean (get p->v const/$_predicate:retractDuplicates))
                                                            :spec               (when (get p->v const/$_predicate:spec) ;; specs are multi-cardinality - if one exists filter through to get all
                                                                                  (extract-spec-ids const/$_predicate:spec pred-flakes))
                                                            :specDoc            (get p->v const/$_predicate:specDoc)
                                                            :txSpec             (when (get p->v const/$_predicate:txSpec) ;; specs are multi-cardinality - if one exists filter through to get all
                                                                                  (extract-spec-ids const/$_predicate:txSpec pred-flakes))
                                                            :txSpecDoc          (get p->v const/$_predicate:txSpecDoc)
                                                            :restrictTag        (get p->v const/$_predicate:restrictTag)
                                                            :fullText           fullText?})
                                               ids       (cond-> (into [id] equivs)
                                                                 p-name (conj p-name)
                                                                 iri (conj iri))]
                                           [(reduce #(assoc %1 %2 p-props) pred ids) ;; create a key for each possible 'id'
                                            (if fullText? (conj fullText id) fullText)]))
                                       [{} #{}]))]
      {:t          (:t db)                                  ;; record time of spec generation, can use to determine cache validity
       :coll       coll
       :pred       pred
       :prefix     (iri-util/system-context (<? prefix-flakes-ch))
       :fullText   fullText
       :subclasses (delay (calc-subclass pred))             ;; delay because might not be needed
       })))

(defn setting-map
  [db]
  (go-try
    (let [setting-flakes (try*
                           (<? (query-range/index-range db :spot = [["_setting/id" "root"]]))
                           (catch* e nil))
          setting-flakes (flake->pred-map setting-flakes)
          settings       {:passwords (boolean (get setting-flakes const/$_setting:passwords))
                          :anonymous (get setting-flakes const/$_setting:anonymous)
                          :language  (get lang-sid->lang (get setting-flakes const/$_setting:language))
                          :ledgers   (get setting-flakes const/$_setting:ledgers)
                          :txMax     (get setting-flakes const/$_setting:txMax)
                          :consensus (get setting-flakes const/$_setting:consensus)}]
      settings)))
