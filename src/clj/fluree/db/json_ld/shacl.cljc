(ns fluree.db.json-ld.shacl
  (:require [fluree.db.util.async :refer [<? go-try]]
            #?(:clj  [fluree.db.util.clj-const :as uc]
               :cljs [fluree.db.util.cljs-const :as uc])
            [fluree.db.util.core :as util :refer [try* catch*]]
            [fluree.db.util.log :as log]
            [fluree.db.json-ld.iri :as iri]
            [fluree.db.query.range :as query-range]
            [fluree.db.constants :as const]
            [fluree.db.flake :as flake]
            [fluree.json-ld :as json-ld]
            [clojure.string :as str]
            [clojure.set :as set]
            [clojure.core.async :as async])
  #?(:clj (:import (java.util.regex Pattern))))

(comment
  ;; a raw SHACL shape looks something like this:
  {#fluree/SID [1 "id"] #fluree/SID [101 "UserShape"]
   #fluree/SID [3 "type"] [#fluree/SID [5 "NodeShape"]]
   #fluree/SID [5 "targetClass"] [#fluree/SID [101 "User"]]
   #fluree/SID [5 "property"]
   [{#fluree/SID [1 "id"] #fluree/SID [24 "fdb-2"]
     #fluree/SID [5 "datatype"] [#fluree/SID [2 "string"]]
     #fluree/SID [5 "maxCount"] [1]
     #fluree/SID [5 "minCount"] [1]
     #fluree/SID [5 "path"] [#fluree/SID [17 "name"]]}

    {#fluree/SID [1 "id"] #fluree/SID [24 "fdb-3"]
     #fluree/SID [5 "maxCount"] [1]
     #fluree/SID [5 "maxInclusive"] [130]
     #fluree/SID [5 "minCount"] [1]
     #fluree/SID [5 "minInclusive"] [0]
     #fluree/SID [5 "path"] [#fluree/SID [17 "age"]]}

    {#fluree/SID [1 "id"] #fluree/SID [24 "fdb-4"]
     #fluree/SID [5 "datatype"] [#fluree/SID [2 "string"]]
     #fluree/SID [5 "path"] [#fluree/SID [17 "email"]]}]}
  ,)

(def numeric-types
  #{const/$xsd:int
    const/$xsd:short
    const/$xsd:float
    const/$xsd:unsignedLong
    const/$xsd:unsignedInt
    const/$xsd:unsignedShort
    const/$xsd:positiveInteger
    const/$xsd:nonPositiveInteger
    const/$xsd:negativeInteger
    const/$xsd:nonNegativeInteger
    const/$xsd:decimal
    const/$xsd:double
    const/$xsd:integer
    const/$xsd:long})

(def time-types
  #{const/$xsd:date
    const/$xsd:dateTime
    const/$xsd:duration
    const/$xsd:gDay
    const/$xsd:gMonth
    const/$xsd:gMonthDay
    const/$xsd:gYear
    const/$xsd:gYearMonth
    const/$xsd:time})

(defn get-regex-flag
  "Given an `sh:flag` value, returns the corresponding regex flag
  for the current platform. If the provided flag is not found,
  it will be ignored by validation.

  Note that js does not have support for `x` or `q` flag behavior."
  [flag]
  #?(:clj  (case flag
             "i" Pattern/CASE_INSENSITIVE
             "m" Pattern/MULTILINE
             "s" Pattern/DOTALL
             "q" Pattern/LITERAL
             "x" Pattern/COMMENTS
             0)
     :cljs (if (#{"i" "m" "s"} flag)
             flag
             "")))

(defn property-shape?
  "Shapes are of two disjoint categories: sh:NodeShape and sh:PropertyShape. If a shape
  has the sh:path as a predicate, it is a sh:PropertyShape."
  [shape]
  (boolean (get shape const/sh_path)))

(defn qualified-value-shape?
  "A qualified value shape has one value for sh:qualifiedValueShape and either a
  sh:qualifiedMinCount or a sh:qualifiedMaxCount."
  [shape]
  (and (first (get shape const/sh_qualifiedValueShape))
       (or (first (get shape const/sh_qualifiedMinCount))
           (first (get shape const/sh_qualifiedMaxCount)))))

(defn build-shape-node
  ([db shape-sid]
   (build-shape-node db shape-sid #{shape-sid}))
  ([db shape-sid built-nodes]
   (go-try
     (let [flakes (<? (query-range/index-range db :spot = [shape-sid]))]
       (if (seq flakes)
         (loop [[f & r] (sort-by (comp :i flake/m) flakes)
                node {const/$id shape-sid}]
           (if f
             (recur r (update node (flake/p f) (fnil conj [])
                              (if (flake/ref-flake? f)
                                (let [ref (flake/o f)]
                                  (if (contains? built-nodes ref)
                                    ref
                                    (<? (build-shape-node db ref (conj built-nodes ref)))))
                                (flake/o f))))
             node))
         shape-sid)))))

(defn build-shape
  [db shape-sid]
  (go-try
    (let [shapes-cache (-> db :schema :shapes)]
      (if-let [shape (get @shapes-cache shape-sid)]
        shape
        (let [shape (<? (build-shape-node db shape-sid))]
          (swap! shapes-cache assoc shape-sid shape)
          shape)))))

(defn build-sibling-shapes
  "Construct the sibling shapes of a shape with a sh:qualifiedValueShape. Siblings are
  other qualified value shape constraints in the same property constraint."
  [shape-db shape]
  (def shape-db shape-db)
  (def shape shape)
  (let [{shape-id const/$id
         [q-disjoint?] const/sh_qualifiedValueShapesDisjoint
         [{q-shape-id const/$id}] const/sh_qualifiedValueShape}
        shape]
    (if q-disjoint?
      (let [parent-shape-id
            (first (async/<!! (query-range/index-range shape-db :opst = [[shape-id const/$xsd:anyURI] const/sh_property]
                                                       {:flake-xf (map flake/s)})))
            sibling-sids
            (async/<!! (query-range/index-range shape-db :spot = [parent-shape-id const/sh_property]
                                                {:flake-xf (map flake/o)}))]
        (loop [[sib-sid & r] sibling-sids
               sib-q-shapes []]
          (if sib-sid
            (recur r (conj sib-q-shapes (async/<!! (build-shape shape-db sib-sid))))
            (->> sib-q-shapes
                 ;; only keep the qualified value shape of the sibling shape
                 (keep #(first (get % const/sh_qualifiedValueShape)))
                 ;; remove original q-shape
                 (remove #(= (get % const/$id) q-shape-id))))))
      [])))

(defmulti validate-constraint
  "A constraint whose focus nodes conform returns nil. A constraint that doesn't returns a
  sequence of result maps."
  (fn [v-ctx shape constraint focus-node value-nodes]
    (println "DEP validate-constraint dispatch" (pr-str constraint))
    constraint))

(defmethod validate-constraint :default [_ _ _ _ _] nil)

(defn validate-constraints
  [v-ctx shape focus-node value-nodes]
  (println "DEP validate-constraints")
  (loop [[[constraint] & r] shape
         results []]
    (if constraint
      (if-let [results* (validate-constraint v-ctx shape constraint focus-node value-nodes)]
        (recur r (into results results*))
        (recur r results))
      (not-empty results))))

(defn sid-node
  [sid]
  [sid const/$xsd:anyURI])

(defn subject-node
  [flake]
  (sid-node (flake/s flake)))

;; TODO: rename to object-node
(defn value-node
  "Take a flake and create a value node suitable for validation. A value node is a tuple of [value dt]."
  [flake]
  [(flake/o flake) (flake/dt flake)])

(defn resolve-predicate-path
  [data-db focus-node pred-path]
  (query-range/index-range data-db :spot = [focus-node pred-path] {:flake-xf (map value-node)}))

(defn resolve-inverse-path
  [data-db focus-node inverse-path]
  (query-range/index-range data-db :opst = [focus-node inverse-path] {:flake-xf (map subject-node)}))

(defn resolve-segment
  [data-db focus-node segment]
  (if (iri/sid? segment)
    (resolve-predicate-path data-db focus-node segment)
    (let [{[inverse-path] const/sh_inversePath} segment]
      (cond inverse-path (resolve-inverse-path data-db focus-node inverse-path)
            :else (throw (ex-info "Unsupported property path segment." {:segment segment}))))))

(defn resolve-value-nodes
  "Return the value nodes resolved via the path from the focus node."
  [data-db focus-node path]
  (println "DEP resolve-value-nodes" (pr-str focus-node) (pr-str path))
  (loop [[segment & segments] path
         focus-nodes [(sid-node focus-node)]
         value-nodes []]
    (if segment
      (let [vns (loop [[[sid :as f-node] & r] focus-nodes
                       v-nodes []]
                  (if f-node
                    (recur r (conj v-nodes (async/<!! (resolve-segment data-db sid segment))))
                    v-nodes))]
        (recur segments
               (apply concat vns)
               vns))
      value-nodes)))

(defn validate-property-shape
  "Returns a sequence of validation results if conforming fails, otherwise nil."
  [{:keys [data-db] :as v-ctx} {path const/sh_path :as shape} focus-node]
  (let [{path const/sh_path} shape]
    (loop [[value-nodes & r] (resolve-value-nodes data-db focus-node path)
           results []]
      (println "DEP property path value-nodes" (pr-str value-nodes))
      (if value-nodes
        (if-let [results* (validate-constraints v-ctx shape focus-node value-nodes)]
          (recur r (into results results*))
          (recur r results))
        (not-empty results)))))

(defn target-node-target?
  [shape s-flakes]
  (let [sid        (some-> s-flakes first flake/s)
        target-sids (->> (get shape const/sh_targetNode) (into #{}))]
    (println "DEP target-node-target?" (pr-str target-sids))
    (contains? target-sids sid)))

(defn target-class-target?
  [shape s-flakes]
  (let [target-class (first (get shape const/sh_targetClass))]
    (println "DEP target-class-target?" (pr-str target-class))
    (some (fn [f]
            (and (flake/class-flake? f)
                 (= (flake/o f) target-class)))
          s-flakes)))

(defn target-subjects-of-target?
  [shape s-flakes]
  (let [target-pid (first (get shape const/sh_targetSubjectsOf))]
    (println "DEP target-subjects-of-target?" (pr-str target-pid))
    (some (fn [f] (= (flake/p f) target-pid))
          s-flakes)))

(defn implicit-target?
  "If a sh:NodeShape has a class it implicitly targets that node."
  ;; https://www.w3.org/TR/shacl/#implicit-targetClass
  [shape s-flakes]
  (let [shape-classes (-> (get shape const/$rdf:type) (set) (disj const/sh_NodeShape))]
    (println "DEP implicit-target?" (pr-str shape-classes))
    (some (fn [f] (and (flake/class-flake? f)
                       (contains? shape-classes (flake/o f))))
          s-flakes)))

(defn target-objects-of-target?
  [shape]
  (first (get shape const/sh_targetObjectsOf)))

(defn target-objects-of-focus-nodes
  [db shape s-flakes]
  (go-try
    (let [target-pid (first (get shape const/sh_targetObjectsOf))]
      (println "DEP target-objects-of-target?" (pr-str target-pid))
      (let [sid             (some-> s-flakes first flake/s)
            referring-pids  (not-empty (<? (query-range/index-range db :opst = [[sid const/$xsd:anyURI]]
                                                                    {:flake-xf (map flake/p)})))
            p-flakes        (filterv (fn [f] (= (flake/p f) target-pid)) s-flakes)
            focus-nodes     (mapv value-node p-flakes)]
        ;; TODO: we don't know that these are sids, so we need to use a node layout for focus nodes
        (cond-> (mapv flake/o p-flakes)
          referring-pids (conj sid))))))

(defn resolve-focus-nodes
  "Evaluate the target declarations of a NodeShape to see if the provided s-flakes contain
  any focus nodes for the shape. Returns a sequence of focus nodes if targets are present."
  [data-db shape s-flakes]
  (go-try
    (let [sid (some-> s-flakes first flake/s)]
      (cond (target-node-target? shape s-flakes)        [sid]
            (target-class-target? shape s-flakes)       [sid]
            (target-subjects-of-target? shape s-flakes) [sid]
            (implicit-target? shape s-flakes)           [sid]
            (target-objects-of-target? shape)
            (<? (target-objects-of-focus-nodes data-db shape s-flakes))
            :else
            ;; no target declaration, no focus nodes
            []))))

(defn validate-node-shape
  ""
  ([{:keys [data-db] :as v-ctx} shape s-flakes]
   (loop [[focus-node & r] (async/<!! (resolve-focus-nodes data-db shape s-flakes))
          results []]
     (if focus-node
       (let [value-nodes (if (= (some-> s-flakes first flake/s) focus-node)
                           (mapv value-node s-flakes)
                           (async/<!! (query-range/index-range data-db :spot = [focus-node] {:flake-xf (map value-node)})))]
         (if-let [results* (validate-node-shape v-ctx shape focus-node value-nodes)]
           (recur r (into results results*))
           (recur r results)))
       (not-empty results))))
  ([v-ctx shape focus-node value-nodes]
   (validate-constraints v-ctx shape focus-node value-nodes)))

(defn base-result
  [{:keys [display] :as v-ctx} shape constraint focus-node]
  (let [{id const/$id
         path const/sh_path
         expect constraint} shape

        [single-expect :as pretty-expect]
        (mapv display expect)]
    (cond-> {:subject (display focus-node)
             :constraint (display constraint)
             :shape (display id)
             :expect (if (> (count expect) 1)
                       pretty-expect
                       single-expect)}
      path (assoc :path (mapv (fn [segment]
                                (if (iri/sid? segment)
                                  (display segment)
                                  (let [[[k [v]]] (seq (dissoc segment const/$id))]
                                    {(display k) (display v)})))
                              path)))))

;; value type constraints
(defmethod validate-constraint const/sh_class [{:keys [display data-db] :as v-ctx} shape constraint focus-node value-nodes]
  (println "DEP validate-constraint " (pr-str constraint))
  (let [{expect constraint} shape

        expected-classes (into #{} expect)

        result (base-result v-ctx shape constraint focus-node)]
    (loop [[[o dt] & r] value-nodes
           results []]
      (if o
        (let [classes (if (iri/sid? o)
                        (->>
                          (async/<!! (query-range/index-range data-db :spot = [o const/$rdf:type]))
                          (into #{} (map flake/o)))
                        #{})
              missing-classes (set/difference expected-classes classes)]
          (recur r (into results
                         (mapv (fn [missing-class]
                                 (assoc result
                                        :value (mapv display classes)
                                        :message (str "missing required class " (display missing-class))))
                               missing-classes))))
        (not-empty results)))))
(defmethod validate-constraint const/sh_datatype [{:keys [display] :as v-ctx} shape constraint focus-node value-nodes]
  (println "DEP validate-constraint " (pr-str constraint))
  (let [{expect constraint} shape

        [datatype] expect
        violations (remove (fn [[_v dt]] (= dt datatype)) value-nodes)]
    (when (not-empty violations)
      [(-> (base-result v-ctx shape constraint focus-node)
           (assoc :value (mapv (comp display second) violations)
                  :message (str "the following values do not have expected datatype " (display datatype) ": "
                                (->> violations
                                     (mapv (fn [[v _dt]] (display v)))
                                     (str/join ",")))))])))
(defmethod validate-constraint const/sh_nodeKind [{:keys [display] :as v-ctx} shape constraint focus-node value-nodes]
  (println "DEP validate-constraint " (pr-str constraint))
  (let [{expect constraint} shape

        [nodekind] expect
        result     (base-result v-ctx shape constraint focus-node)]
    (->> value-nodes
         (remove (fn [[v _dt]]
                   (let [iri?     (and (iri/sid? v) (not (iri/bnode? v)))
                         bnode?   (iri/bnode? v)
                         literal? (not (iri/sid? v))]
                     (condp = nodekind
                       const/sh_BlankNode          bnode?
                       const/sh_IRI                iri?
                       const/sh_BlankNodeOrIRI     (or iri? bnode?)
                       const/sh_IRIOrLiteral       (or iri? literal?)
                       const/sh_BlankNodeOrLiteral (or bnode? literal?)))))
         (mapv (fn [[v _dt]]
                 (let [value (display v)]
                   (assoc result
                          :value value
                          :message (str "value " value " is is not of kind " (display nodekind)))))))))

;; cardinality constraints
(defmethod validate-constraint const/sh_minCount [{:keys [display] :as v-ctx} shape constraint focus-node value-nodes]
  (println "DEP validate-constraint " (pr-str constraint))
  (let [{expect constraint} shape

        [min] expect
        n     (count value-nodes)]
    (when (< n min)
      [(-> (base-result v-ctx shape constraint focus-node)
           (assoc :value n
                  :message (str "count " n " is less than minimum count of " min)))])))
(defmethod validate-constraint const/sh_maxCount [{:keys [display] :as v-ctx} shape constraint focus-node value-nodes]
  (println "DEP validate-constraint " (pr-str constraint) )
  (let [{expect constraint} shape

        [max] expect
        n     (count value-nodes)]
    (when (> n max)
      [(-> (base-result v-ctx shape constraint focus-node)
           (assoc :value n
                  :message (str "count " n " is greater than maximum count of " max)))])))

;; value range constraints
(defmethod validate-constraint const/sh_minExclusive [{:keys [display] :as v-ctx} shape constraint focus-node value-nodes]
  (println "DEP validate-constraint " (pr-str constraint))
  (let [{expect constraint} shape

        [min-ex] expect
        result   (base-result v-ctx shape constraint focus-node)]
    (->> value-nodes
         (remove (fn [[v dt]]
                   (and (contains? numeric-types dt)
                        (> v min-ex))))
         (mapv (fn [[v dt]]
                 (let [value (display v)]
                   (assoc result
                          :value value
                          :message (str "value " value " is less than exclusive minimum " min-ex))))))))
(defmethod validate-constraint const/sh_maxExclusive [{:keys [display] :as v-ctx} shape constraint focus-node value-nodes]
  (println "DEP validate-constraint " (pr-str constraint))
  (let [{expect constraint} shape

        [max-ex] expect
        result  (base-result v-ctx shape constraint focus-node)]
    (->> value-nodes
         (remove (fn [[v dt]]
                   (and (contains? numeric-types dt)
                        (< v max-ex))))
         (mapv (fn [[v _dt]]
                 (let [value (display v)]
                   (assoc result
                          :value value
                          :message (str "value " value " is greater than exclusive maximum " max-ex))))))))
(defmethod validate-constraint const/sh_minInclusive [{:keys [display] :as v-ctx} shape constraint focus-node value-nodes]
  (println "DEP validate-constraint " (pr-str constraint))
  (let [{expect constraint} shape

        [min-in] expect
        result   (base-result v-ctx shape constraint focus-node)]
    (->> value-nodes
         (remove (fn [[v dt]]
                   (and (contains? numeric-types dt)
                        (>= v min-in))))
         (mapv (fn [[v _dt]]
                 (let [value (display v)]
                   (assoc result
                          :value value
                          :message (str "value " value " is less than inclusive minimum " min-in))))))))
(defmethod validate-constraint const/sh_maxInclusive [{:keys [display] :as v-ctx} shape constraint focus-node value-nodes]
  (println "DEP validate-constraint " (pr-str constraint))
  (let [{expect constraint} shape

        [max-in] expect
        result   (base-result v-ctx shape constraint focus-node)]
    (->> value-nodes
         (remove (fn [[v dt]]
                   (and (contains? numeric-types dt)
                        (<= v max-in))))
         (mapv (fn [[v _dt]]
                 (let [value (display v)]
                   (assoc result
                          :value value
                          :message (str "value " value " is greater than inclusive maximum " max-in))))))))

;; string-based constraints
(defmethod validate-constraint const/sh_minLength [{:keys [display] :as v-ctx} shape constraint focus-node value-nodes]
  (println "DEP validate-constraint " (pr-str constraint))
  (let [{expect constraint} shape

        [min-length] expect
        result       (base-result v-ctx shape constraint focus-node)]
    (->> value-nodes
         (remove (fn [[v _dt]] (>= (count (str v)) min-length)))
         (mapv (fn [[v _dt]]
                 (if (iri/sid? v)
                   (let [value (display v)]
                     (assoc result
                            :value v
                            :message (str "value " (pr-str value) " is not a literal value")))
                   (let [value (pr-str (str v))]
                     (assoc result
                            :value v
                            :message (str "value " value " has string length less than minimum length " min-length)))))))))
(defmethod validate-constraint const/sh_maxLength [{:keys [display] :as v-ctx} shape constraint focus-node value-nodes]
  (println "DEP validate-constraint " (pr-str constraint))
  (let [{expect constraint} shape

        [max-length] expect
        result       (base-result v-ctx shape constraint focus-node)]
    (->> value-nodes
         (remove (fn [[v _dt]] (<= (count (str v)) max-length)))
         (mapv (fn [[v _dt]]
                 (if (iri/sid? v)
                   (let [value (display v)]
                     (assoc result
                            :value v
                            :message (str "value " value " is not a literal value")))
                   (let [value (pr-str (str v))]
                     (assoc result
                            :value v
                            :message (str "value " value " has string length greater than maximum length " max-length)))))))))
(defmethod validate-constraint const/sh_pattern [{:keys [display] :as v-ctx} shape constraint focus-node value-nodes]
  (println "DEP validate-constraint " (pr-str constraint))
  (let [{expect constraint flags const/sh_flags} shape

        [pattern-str] expect
        valid-flags   (mapv get-regex-flag flags)
        pattern       #?(:clj (Pattern/compile pattern-str (apply + valid-flags))
                         :cljs (js/RegExp. pattern-str (apply str valid-flags)))
        result        (base-result v-ctx shape constraint focus-node)]
    (println "DEP flags" (pr-str flags) (pr-str valid-flags))
    (->> value-nodes
         (remove (fn [[v dt]] (re-find pattern (str v))))
         (mapv (fn [[v _dt]]
                 (let [value (display v)]
                   (assoc result
                          :value v
                          :message (str "value " (pr-str (str value)) " does not match pattern " (pr-str pattern-str)
                                        (when (seq valid-flags)
                                          (str " with " (display const/sh_flags) " " (str/join ", " flags)))))))))))
#_(defmethod validate-constraint const/sh_languageIn [v-ctx constraint focus-flakes]) ; not supported
#_(defmethod validate-constraint const/sh_uniqueLang [v-ctx constraint focus-flakes]) ; not supported

;; property pair constraints
(defmethod validate-constraint const/sh_equals [{:keys [display data-db] :as v-ctx} shape constraint focus-node value-nodes]
  (println "DEP validate-constraint " (pr-str constraint))
  (let [{expect constraint} shape

        [equals]       expect
        equals-flakes  (async/<!! (query-range/index-range data-db :spot = [focus-node equals]))
        equals-objects (into #{} (map flake/o) equals-flakes)
        focus-objects  (into #{} (map first) value-nodes)]
    (when (not= equals-objects focus-objects)
      (let [result (base-result v-ctx shape constraint focus-node)
            iri-path (:path result)
            expect-vals  (sort (mapv display equals-objects))
            values   (sort (mapv (fn [[v _dt]] (display v)) value-nodes))]
        [(-> result
             (assoc :value values
                    :expect expect-vals
                    :message (str "path " iri-path " values " (str/join ", " values) " do not equal "
                                  (display equals) " values " (str/join ", " expect-vals))))]))))
(defmethod validate-constraint const/sh_disjoint [{:keys [data-db display] :as v-ctx} shape constraint focus-node value-nodes]
  (println "DEP validate-constraint " (pr-str constraint))
  (let [{expect constraint} shape

        [disjoint]       expect
        disjoint-flakes  (async/<!! (query-range/index-range data-db :spot = [focus-node disjoint]))
        disjoint-objects (into #{} (map flake/o) disjoint-flakes)
        focus-objects    (into #{} (map first) value-nodes)]
    (when (not-empty (set/intersection focus-objects disjoint-objects))
      (let [result      (base-result v-ctx shape constraint focus-node)
            iri-path    (:path result)
            expect-vals (sort (mapv display disjoint-objects))
            values      (sort (mapv (fn [[v _dt]] (display v)) value-nodes))]
        [(-> result
             (assoc :value values
                    :expect expect-vals
                    :message (str "path " iri-path " values " (str/join ", " values) " are not disjoint with "
                                  (display disjoint) " values " (str/join ", " expect-vals))))]))))
(defmethod validate-constraint const/sh_lessThan [{:keys [data-db display] :as v-ctx} shape constraint focus-node value-nodes]
  (println "DEP validate-constraint " (pr-str constraint))
  (let [{expect constraint} shape

        [less-than]       expect
        less-than-flakes  (async/<!! (query-range/index-range data-db :spot = [focus-node less-than]))
        less-than-objects (into #{} (map flake/o) less-than-flakes)
        focus-objects     (into #{} (map first) value-nodes)

        result      (base-result v-ctx shape constraint focus-node)
        iri-path    (:path result)
        expect-vals (sort (mapv display less-than-objects))
        values      (sort (mapv (fn [[v _dt]] (display v)) value-nodes))

        result (assoc result :value values :expect expect-vals)]
    (if (or (and (every? (fn [f] (contains? numeric-types (flake/dt f))) less-than-flakes)
                 (every? (fn [[v dt]] (contains? numeric-types dt)) value-nodes))
            (and (every? (fn [f] (contains? time-types (flake/dt f))) less-than-flakes)
                 (every? (fn [[v dt]] (contains? time-types dt)) value-nodes)))
      (when-not (every? (fn [o] (apply < o (sort less-than-objects))) focus-objects)
        [(assoc result :message (str "path " iri-path " values " (str/join ", " values) " are not all less than "
                                     (display less-than) " values " (str/join ", " expect-vals)))])
      [(assoc result :message (str "path " iri-path " values " (str/join ", " values) " are not all comparable with "
                                   (display less-than) " values " (str/join ", " expect-vals)))])))
(defmethod validate-constraint const/sh_lessThanOrEquals [{:keys [data-db display] :as v-ctx} shape constraint focus-node value-nodes]
  (println "DEP validate-constraint " (pr-str constraint))
  (let [{expect constraint} shape

        [less-than]       expect
        less-than-flakes  (async/<!! (query-range/index-range data-db :spot = [focus-node less-than]))
        less-than-objects (into #{} (map flake/o) less-than-flakes)
        focus-objects     (into #{} (map first) value-nodes)

        result      (base-result v-ctx shape constraint focus-node)
        iri-path    (:path result)
        expect-vals (sort (mapv display less-than-objects))
        values      (sort (mapv (fn [[v _dt]] (display v)) value-nodes))
        result      (assoc result :value values :expect expect-vals)]
    (if (or (and (every? (fn [f] (contains? numeric-types (flake/dt f))) less-than-flakes)
                 (every? (fn [[_ dt]] (contains? numeric-types dt)) value-nodes))
            (and (every? (fn [f] (contains? time-types (flake/dt f))) less-than-flakes)
                 (every? (fn [[_ dt]] (contains? time-types dt)) value-nodes)))
      (when-not (every? (fn [o] (apply <= o (sort less-than-objects))) focus-objects)
        [(assoc result :message (str "path " iri-path " values " (str/join ", " values) " are not all less than "
                                     (display less-than) " values " (str/join ", " expect-vals)))])
      [(assoc result :message (str "path " iri-path " values " (str/join ", " values) " are not all comparable with "
                                   (display less-than) " values " (str/join ", " expect-vals)))])))

;; logical constraints
(defmethod validate-constraint const/sh_not [{:keys [display] :as v-ctx} shape constraint focus-node value-nodes]
  (println "DEP validate-constraint " (pr-str constraint))
  (loop [[p-shape & r] (get shape const/sh_not)
         results []]
    (if p-shape
      (if-let [results* (validate-property-shape v-ctx p-shape focus-node)]
        (recur r results)
        (recur r (conj results (-> (base-result v-ctx shape constraint focus-node)
                                   (dissoc :expect)
                                   (assoc
                                     :value (display focus-node)
                                     :message (str (display focus-node) " conforms to shape " (display (get p-shape const/$id))))))))
      (not-empty results))))
(defmethod validate-constraint const/sh_and [v-ctx constraint focus-flakes]
  (println "DEP validate-constraint " (pr-str constraint)))
(defmethod validate-constraint const/sh_or [v-ctx constraint focus-flakes]
  (println "DEP validate-constraint " (pr-str constraint)))
(defmethod validate-constraint const/sh_xone [v-ctx constraint focus-flakes]
  (println "DEP validate-constraint " (pr-str constraint)))

;; shape-based constraints
(defmethod validate-constraint const/sh_node [{:keys [display data-db shape-db] :as v-ctx} shape constraint focus-node value-nodes]
  (println "DEP validate-constraint " (pr-str constraint))
  (let [{expect constraint} shape

        pretty-expect (->> expect
                           (mapv #(get % const/$id))
                           (mapv display))
        result (-> (base-result v-ctx shape constraint focus-node)
                   (assoc :expect pretty-expect))]
    (println "DEP sh:node expect" (pr-str expect))
    (loop [[[v dt] & r] value-nodes
           results []]
      (if (some? v)
        (if (iri/sid? v)
          (recur r (into results
                         (loop [[node-shape & r] expect
                                results []]
                           (if node-shape
                             (let [value-nodes (async/<!! (query-range/index-range data-db :spot = [v] {:flake-xf (map value-node)}))]
                               (println "DEP node shape" (pr-str shape))
                               (if-let [results* (validate-node-shape v-ctx node-shape v value-nodes)]
                                 (recur r (conj results (assoc result
                                                               :value (display v)
                                                               :message (str "node " (display v) " does not conform to shapes "
                                                                             pretty-expect))))
                                 (recur r results)))
                             results))))
          (recur r (conj results (assoc result
                                        :value v
                                        :message (str "value " v " does not conform to shapes "
                                                      pretty-expect)))))
        (not-empty results)))))
(defmethod validate-constraint const/sh_property [v-ctx shape constraint focus-node value-nodes]
  (println "DEP validate-constraint " (pr-str constraint))
  (loop [[p-shape & r] (get shape const/sh_property)
         results []]
    (if p-shape
      (if-let [results* (validate-property-shape v-ctx p-shape focus-node)]
        (recur r (into results results*))
        (recur r results))
      (not-empty results))))
(defmethod validate-constraint const/sh_qualifiedValueShape [{:keys [display data-db shape-db] :as v-ctx}
                                                             shape constraint focus-node value-nodes]
  (println "DEP validate-constraint " (pr-str constraint))
  (def v-ctx v-ctx)
  (let [{expect constraint
         [q-disjoint?] const/sh_qualifiedValueShapesDisjoint
         [q-min-count] const/sh_qualifiedMinCount
         [q-max-count] const/sh_qualifiedMaxCount} shape

        [q-shape] expect

        values (->> value-nodes
                    (mapv first)
                    (mapv display))

        result (-> (base-result v-ctx shape constraint focus-node)
                   (assoc :expect (display (get q-shape const/$id)))
                   (assoc :value values))]
    (println "DEP q-shape" (pr-str q-shape))
    (loop [[[v _dt] & r] value-nodes
           conforming []]
      (println "DEP flake " (pr-str v) (pr-str conforming))
      (if (some? v)
        ;; build up conforming sids
        (let [focus-node* (if (iri/sid? v) v focus-node)
              value-nodes* (if (iri/sid? v)
                             (async/<!! (query-range/index-range data-db :spot = [v] {:flake-xf (map value-node)}))
                             value-nodes)
              result (if (property-shape? q-shape)
                       (validate-property-shape v-ctx q-shape focus-node*)
                       (validate-node-shape v-ctx q-shape focus-node* value-nodes*))]
          (println "DEP q-shape result" (pr-str result))
          (if result
            (recur r conforming)
            (recur r (conj conforming v))))

        (do
          (println "DEP end flake" (pr-str q-disjoint?) (pr-str conforming))
          (if q-disjoint?
            ;; disjoint requires subjects that conform to this q-shape cannot conform to any of the sibling q-shapes
            (let [sibling-q-shapes (build-sibling-shapes shape-db shape)]
              (loop [[conforming-sid & r] conforming
                     non-disjoint-conformers #{}]
                (println "DEP conforming-sid" (pr-str conforming-sid) (pr-str non-disjoint-conformers))
                (if conforming-sid
                  (recur r
                         (loop [[sib-q-shape & r] sibling-q-shapes
                                non-disjoint-conformers* []]
                           (println "DEP sib-q-shape" (pr-str sib-q-shape) (pr-str non-disjoint-conformers*))
                           (if sib-q-shape
                             (let [value-nodes (async/<!! (query-range/index-range data-db :spot = [conforming-sid]
                                                                                   {:flake-xf (map value-node)}))
                                   q-result (if (property-shape? sib-q-shape)
                                              (validate-property-shape v-ctx sib-q-shape conforming-sid)
                                              (validate-node-shape v-ctx sib-q-shape conforming-sid value-nodes))]
                               (println "DEP sib-q-shape result" (pr-str q-result) )
                               (if q-result
                                 (recur r non-disjoint-conformers*)
                                 (recur r (conj non-disjoint-conformers* conforming-sid))))
                             (do
                               (println "DEP sib-q-shape end" (pr-str non-disjoint-conformers*))
                               (into non-disjoint-conformers non-disjoint-conformers*)))))


                  (do
                    (println "DEP conforming-sid end" (pr-str non-disjoint-conformers))
                    (if (not-empty non-disjoint-conformers)
                      ;; each non-disjoint sid produces a validation result
                      (mapv
                        (fn [non-disjoint-sid]
                          (assoc result
                                 :value (display non-disjoint-sid)
                                 :message (str "value " (display non-disjoint-sid) " conformed to a sibling qualified value shape "
                                               (mapv #(display (get % const/$id)) sibling-q-shapes) " in violation of the "
                                               (display const/sh_qualifiedValueShapesDisjoint) " constraint")))

                        non-disjoint-conformers)

                      ;; no non-disjoint conformers, validate count constraints
                      (cond (and q-min-count (< (count conforming) q-min-count))
                            [(assoc result
                                    :message (str "values " values " conformed to " (display (get q-shape const/$id))
                                                  " less than " (display const/sh_qualifiedMinCount) " " q-min-count " times"))]
                            (and q-max-count (> (count conforming) q-max-count))
                            [(assoc result
                                    :message (str "values " values " conformed to " (display (get q-shape const/$id))
                                                  " more than " (display const/sh_qualifiedMaxCount) " " q-max-count " times"))]))))))
            ;; validate count constraints
            (do
              (println "DEP no disjoint check")
              (cond (and q-min-count (< (count conforming) q-min-count))
                    [(assoc result
                            :message (str "values " values " conformed to " (display (get q-shape const/$id))
                                          " less than " (display const/sh_qualifiedMinCount) " " q-min-count " times"))]
                    (and q-max-count (> (count conforming) q-max-count))
                    [(assoc result
                            :message (str "values " values " conformed to " (display (get q-shape const/$id))
                                          " more than " (display const/sh_qualifiedMaxCount) " " q-max-count " times"))]))))))))

;; other constraints
(defmethod validate-constraint const/sh_closed [{:keys [data-db display] :as v-ctx} shape constraint focus-node value-nodes]
  (println "DEP validate-constraint " (pr-str constraint))
  (let [{expect constraint ignored const/sh_ignoredProperties
         properties const/sh_property} shape

        [closed?]   expect
        s-flakes    (async/<!! (query-range/index-range data-db :spot = [focus-node]))
        constrained (into #{} (map #(-> % (get const/sh_path) first) properties))
        allowed     (into constrained ignored)
        present     (into #{} (map flake/p) s-flakes)
        not-allowed (set/difference present allowed)]
    (println "DEP closed" (pr-str allowed) (pr-str present) (pr-str not-allowed))
    (when (not-empty not-allowed)
      (let [pid->flakes (group-by flake/p s-flakes)]
        (mapv (fn [path]
                (let [values (->> (get pid->flakes path)
                                  (mapv #(if (flake/ref-flake? %)
                                           (display (flake/o %))
                                           (flake/o %))))]
                  (-> (base-result v-ctx shape constraint focus-node)
                      (assoc :value values
                             :expect (mapv display allowed)
                             :message (str "disallowed path " (display path) " with values " (str/join "," values))))))
              not-allowed)))))
(defmethod validate-constraint const/sh_hasValue [{:keys [display] :as v-ctx} shape constraint focus-node value-nodes]
  (println "DEP validate-constraint " (pr-str constraint))
  (let [{expect constraint} shape

        [term] expect]
    (when-not (some #(= term (first %)) value-nodes)
      (let [value (mapv (fn [[v _dt]] (display v)) value-nodes)
            expect (display term)]
        [(-> (base-result v-ctx shape constraint focus-node)
             (assoc :value value
                    :message (str "at least one value of " value " must be " expect)))]))))
(defmethod validate-constraint const/sh_in [{:keys [display] :as v-ctx} shape constraint focus-node value-nodes]
  (println "DEP validate-constraint " (pr-str constraint))
  (let [{expect constraint} shape

        expected (into #{} expect)
        result   (base-result v-ctx shape constraint focus-node)]
    (->> value-nodes
         (remove (fn [[v dt]] (contains? expected v)))
         (mapv (fn [[v dt]]
                 (let [value (display v)]
                   (assoc result
                          :value v
                          :message (str "value " (pr-str value) " is not in " (:expect result)))))))))

(defn explain-result
  [{:keys [subject constraint shape path message]}]
  (str "Subject " subject (when path (str " path " path))
       " violates constraint " constraint " of shape " shape " - " message "."))

(defn throw-shacl-violation
  [{ns-codes :namespace-codes} context results]
  (println "DEP throw-shacl-violation" (count results))
  (def r results)
  (let [message (->> (mapv explain-result results)
                     (str/join "\n"))]
    (throw (ex-info message
                    {:status 400
                     :error  :shacl/violation
                     :report results}))))

(defn all-node-shape-ids
  [db]
  (def db db)
  (query-range/index-range db :post = [const/$rdf:type [const/sh_NodeShape const/$xsd:anyURI]]
                           {:flake-xf (map flake/s)}))

(defn sid->compact-iri
  [ns-codes context sid]
  (-> (iri/sid->iri sid ns-codes)
      (json-ld/compact context)))

(defn make-display
  [data-db context]
  (fn [v]
    (if (iri/sid? v)
      (sid->compact-iri (:namespace-codes data-db) context v)
      v)))

(defn validate!
  "Will throw an exception if any of the modified subjects fails to conform to a shape that targets it.

  The `shape-db` is the db-before, since newly transacted shapes are not applied to the
  transaction they appear in. The `data-db` is the db after, and it has to conform to
  the shapes in the shape-db.

  `modified-subjects` is a sequence of s-flakes of modified subjects."
  [shape-db data-db modified-subjects context]
  (def sg shape-db)
  (def dg data-db)
  (def mods modified-subjects)
  (println "DEP validate!" (pr-str modified-subjects))
  (go-try
    (doseq [s-flakes modified-subjects]
      (doseq [shape-sid (<? (all-node-shape-ids shape-db))]
        (let [subject (-> s-flakes first flake/s)
              shape   (<? (build-shape shape-db shape-sid))
              v-ctx   {:display  (make-display data-db context)
                       :shape-db shape-db
                       :data-db  data-db}]
          (println "DEP shape" (pr-str shape-sid))
          ;; only enforce activated shapes
          (when (not (get shape const/sh_deactivated))
            (let [results (validate-node-shape v-ctx shape s-flakes)]
              (println "DEP report" (pr-str results))
              (when results
                (throw-shacl-violation data-db context results)))))))))
