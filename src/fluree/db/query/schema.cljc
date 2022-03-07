(ns fluree.db.query.schema
  (:require [fluree.db.flake :as flake]
            [fluree.db.dbproto :as dbproto]
            [fluree.db.constants :as const]
            [fluree.db.util.async :refer [<? go-try]]
            [fluree.db.query.range :as query-range]
            [fluree.db.util.core :as util :refer [try* catch*]]
            [fluree.db.util.iri :as iri-util]
            [fluree.db.util.schema :as schema-util]))

#?(:clj (set! *warn-on-reflection* true))

(defn pred-name->keyword
  "Takes an predicate name (string) and returns the namespace portion of it as a keyword."
  [pred-name]
  (when (string? pred-name)
    (-> (re-find #"[^/]+$" pred-name)                       ;; take everything after the '/'
        keyword)))


(defn- convert-type-to-kw
  "Converts a tag sid for a _predicate/type attributes into a keyword of just the 'name'."
  [type-tag-sid db]
  (go-try
    (-> (<? (dbproto/-tag db type-tag-sid "_predicate/type"))
        (keyword))))

(defn pred-objects-unique?
  [db pred-id]
  (go-try
    (let [os (->> (query-range/index-range db :psot = [pred-id])
                  (<?)
                  (map #(flake/o %)))]
      (if (and os (not (empty? os)))
        (apply distinct? os) true))))

(defn new-pred-changes
  "Returns a map of predicate changes with their respective old
  value and new value, both the key and value of the map are two-tuples as follows:
  {subid  {:new?  true
          :type  {:old :int :new :long}
          :index {:old nil  :new true }}}

  If the predicate being changed is the :type, it resolves the type _tag to its short keyword name

  When an old value does not exist, old-val is nil.
  If they subject being created is completely new, :new? true "
  [db tempids flakes filter?]
  (go-try
    (let [pred-flakes (if filter?
                        (filter schema-util/is-pred-flake? flakes)
                        flakes)
          is-new?     (into #{} (vals tempids))             ;; a set of all the new tempid subids, to be used as a fn
          new-map     (reduce
                        #(let [f %2]
                           (assoc-in %1 [(flake/s f) :new?]
                                     (boolean (is-new? (flake/s f)))))
                        {} pred-flakes)]
      (loop [[f & r] pred-flakes
             acc new-map]
        (if-not f
          acc
          (let [pid         (flake/p f)
                pid-keyword (-> (dbproto/-p-prop db :name pid) (pred-name->keyword))
                old-val?    (false? (flake/op f))
                v           (if (= :type pid-keyword)
                              (<? (convert-type-to-kw (flake/o f) db))
                              (flake/o f))]
            (recur r (if old-val?
                       (assoc-in acc [(flake/s f) pid-keyword :old] v)
                       (assoc-in acc [(flake/s f) pid-keyword :new] v)))))))))


(defn type-error
  "Throw an error if schema update attempt is invalid."
  ([current-type new-type throw?]
   (type-error nil current-type new-type throw?))
  ([db current-type new-type throw?]
   (let [message (str "Cannot convert an _predicate from " (name current-type) " to " (name new-type) ".")]
     (if throw?
       (throw (ex-info message
                       {:status 400
                        :error  :db/invalid-tx}))
       db))))


;; TODO - refactor!
(defn predicate-change-error
  "Accepts a db (should have root permissions) and a map of predicate changes as produced by new-pred-changes.
  Returns a db with updated idxs if relevant, i.e. if non-unique predicate converted to unique
  If optional throw? parameter is true, will throw with an ex-info error."
  ([pred-changes db] (predicate-change-error pred-changes db false))
  ([pred-changes db throw?]
   (go-try
     (loop [[[pred-id changes] & r] pred-changes
            db db]
       (if-not pred-id
         db
         (let [;; TODO - use smart functions?
               db* (if (and
                         (:multi changes)
                         (false? (:new (:multi changes)))   ;; check for explicitly false, not nil
                         (true? (:old (:multi changes))))
                     (type-error db "multi-cardinality" "single-cardinality" throw?)
                     db)

               ;; TODO - use smart functions?
               ;; :unique cannot be set to true if type is boolean, cannot change from anything to boolean,
               ;; so only need to check new predicates
               db* (if (and
                         (:unique changes)
                         (:type changes)
                         (true? (:new? changes))
                         (= :boolean (:new (:type changes)))
                         (true? (:new (:unique changes))))
                     (if throw?
                       (throw (ex-info (str "A boolean _predicate cannot be unique.")
                                       {:status 400
                                        :error  :db/invalid-tx}))
                       db*)
                     db*)

               ;; TODO - use smart functions?
               ;; :component cannot be set to true for an existing subject (it can be set to false).
               db* (if (and
                         (:component changes)
                         (not (:new? changes))
                         (true? (:new (:component changes))))
                     (type-error db* "a component" "a non-component" throw?)
                     db*)

               ;; :unique cannot be set to true for existing predicate if existing values are not unique
               db* (if (and
                         (:unique changes)
                         (not (:new? changes))
                         (true? (:new (:unique changes)))
                         (not (<? (pred-objects-unique? db pred-id))))
                     (if throw?
                       (throw (ex-info (str "The _predicate " (dbproto/-p-prop db :name pred-id) " cannot be set to unique, because there are existing non-unique values.")
                                       {:status 400
                                        :error  :db/invalid-tx}))
                       db*) db*)

               db* (if (and (:type changes)                 ;; must be setting the predicate :type
                            (:old (:type changes)))
                     (let [{:keys [old new]} (:type changes)]
                       (cond
                         (= new old)
                         db*

                         ;; These types cannot be converted into anything else - and float?
                         (#{:string :bigint :bigdec} old)
                         (type-error old new throw?)

                         :else (case new

                                 ;; a long can only be converted from an int or instant
                                 :long (if (#{:int :instant} old)
                                         db* (type-error old new throw?))

                                 ;; BigIntegers can only be converted from an int, long, or instant
                                 :bigint (if (#{:int :long :instant} old)
                                           db* (type-error old new throw?))

                                 ;; a double can only be converted from a float, long, or int
                                 :double (if (#{:float :long :int} old)
                                           db* (type-error old new throw?))

                                 ;; a float can only be converted from an int, long, or float
                                 :float (if (#{:int :float :long} old)
                                          db* (type-error old new throw?))

                                 ;; BigDecimals can only be converted from a float, double, int, long, biginteger
                                 :bigdec (if (#{:float :double :int :long :bigint} old)
                                           db* (type-error old new throw?))

                                 ;; Strings can be converted from json, geojson, bytes, uuid, uri
                                 :string (if (#{:json :geojson :bytes :uuid :uri} old)
                                           db* (type-error old new throw?))

                                 ;; an instant can be converted from a long or int.
                                 :instant (if (#{:long :int} old)
                                            db* (type-error old new throw?))

                                 ;; else don't allow any other changes
                                 (type-error old new throw?))))
                     db*)

               ;; TODO - use collection spec
               ;; If new subject, has to specify type. If it has :component true, then :type needs to be ref
               db* (if (and
                         (true? (:new? changes))
                         (:component changes)
                         (true? (:new (:component changes)))
                         (not (= :ref (:new (:type changes)))))
                     (if throw?
                       (throw (ex-info (str "A component _predicate must be of type \"ref.\"")
                                       {:status 400
                                        :error  :db/invalid-tx}))
                       db*)
                     db*)]
           (recur r db*)))))))


(defn validate-schema-change
  ([db tempids flakes] (validate-schema-change db tempids flakes true))
  ([db tempids flakes filter?]
   (go-try
     (let [changes (<? (new-pred-changes db tempids flakes filter?))]
       (if (empty? changes)
         db
         (<? (predicate-change-error changes db true)))))))


(def predicate-re #"(?:([^/]+)/)([^/]+)")
(def pred-reverse-ref-re #"(?:([^/]+)/)_([^/]+)")

(defn reverse-ref?
  "Reverse refs must be strings that include a '/_' in them, which characters before and after."
  ([predicate-name]
   (reverse-ref? predicate-name false))
  ([predicate-name throw?]
   (if (string? predicate-name)
     (boolean (re-matches pred-reverse-ref-re predicate-name))
     (if throw?
       (throw (ex-info (str "Bad predicate name, should be string: " (pr-str predicate-name))
                       {:status 400
                        :error  :db/invalid-predicate}))
       false))))


(defn reverse-ref
  "Reverses an predicate name."
  [predicate-name]
  (if (string? predicate-name)
    (let [[_ ns name] (re-matches #"(?:([^/]+)/)?([^/]+)" predicate-name)]
      (if ns
        (if (= \_ (nth name 0))
          (str ns "/" (subs name 1))
          (str ns "/_" name))
        (throw (ex-info (str "Bad predicate name, does not contain a namespace portion: " (pr-str predicate-name))
                        {:status 400
                         :error  :db/invalid-predicate}))))
    (throw (ex-info (str "Bad predicate name, should be string: " (pr-str predicate-name))
                    {:status 400
                     :error  :db/invalid-predicate}))))


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
  (reduce (fn [acc flake]                                   ;; quick lookup map of predicate's predicate ids
            (let [p         (flake/p flake)
                  o         (flake/o flake)
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
       (keep #(let [f %]
                (when (= spec-pid (flake/p f)) (flake/o f))))
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


(defn is-class?
  "Returns true if _predicate value is a class, else assumed to be a property/predicate.

  Takes predicate->value map as input."
  [p->v]
  (when-let [type (get p->v const/$rdf:type)]
    (#{const/$rdfs:Class const/$owl:Class} type)))


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
          [collection-flakes predicate-flakes] (partition-by #(<= (flake/s %) flake/MAX-COLL-SUBJECTS)
                                                             schema-flakes)
          coll             (->> collection-flakes
                                (partition-by #(flake/s %))
                                (reduce (fn [acc coll-flakes]
                                          (let [first-flake (first coll-flakes)
                                                sid       (flake/s first-flake)
                                                p->v      (->> coll-flakes ;; quick lookup map of collection's predicate ids
                                                               (reduce #(let [f %2]
                                                                          (assoc %1 (flake/p f) (flake/o f)))
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
                                        {-1    {:name "_tx" :id -1 :sid -1 :partition -1 :spec nil :specDoc nil}
                                         "_tx" {:name "_tx" :id -1 :sid -1 :partition -1 :spec nil :specDoc nil}}))
          [pred fullText] (->> predicate-flakes
                               (partition-by #(flake/s %))
                               (reduce (fn [[pred fullText] pred-flakes]
                                         (let [first-flake (first pred-flakes)
                                               id        (flake/s first-flake)
                                               p->v      (flake->pred-map pred-flakes)
                                               class?    (is-class? p->v)
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

(defn version
  "Returns schema version from a db, which is the :t when the schema was last updated."
  [db]
  (get-in db [:schema :t]))
