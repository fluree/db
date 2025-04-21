(ns fluree.db.flake.format
  (:require [clojure.core.async :as async :refer [go]]
            [fluree.db.constants :as const]
            [fluree.db.flake :as flake]
            [fluree.db.json-ld.iri :as iri]
            [fluree.db.query.exec.select.subject :as subject]
            [fluree.db.query.range :as query-range]
            [fluree.db.track.fuel :as fuel]
            [fluree.db.util.core :as util]
            [fluree.db.util.json :as json]))

#?(:clj (set! *warn-on-reflection* true))

(defn cache-sid->iri
  [db cache compact-fn sid]
  (let [cache-key [(:alias db) sid]]
    (or (get @cache cache-key)
        (when-let [iri (or (some-> db :schema :pred (get sid) :iri compact-fn)
                           (some-> (iri/decode-sid db sid) compact-fn))]
          (vswap! cache assoc cache-key {:as iri})
          {:as iri}))))

(defn flake-bounds
  [db idx match]
  (let [[start-test start-match end-test end-match]
        (query-range/expand-range-interval idx = match)

        [s1 p1 o1 t1 op1 m1]
        (query-range/match->flake-parts db idx start-match)

        [s2 p2 o2 t2 op2 m2]
        (query-range/match->flake-parts db idx end-match)

        start-flake (query-range/resolve-match-flake start-test s1 p1 o1 t1 op1 m1)
        end-flake   (query-range/resolve-match-flake end-test s2 p2 o2 t2 op2 m2)]
    [start-flake end-flake]))

(defn rdf-type?
  [pid]
  (= const/$rdf:type pid))

(defn list-element?
  [flake]
  (-> flake flake/m (contains? :i)))

(defn type-value
  [db cache compact-fn type-flakes]
  (->> type-flakes
       (into [] (comp (map flake/o)
                      (map (partial cache-sid->iri db cache compact-fn))
                      (map :as)))
       util/unwrap-singleton))

(defn format-reference
  [db spec sid]
  (let [iri (iri/decode-sid db sid)]
    (subject/encode-reference iri spec)))

(defn format-object
  [db spec f]
  (let [obj (flake/o f)
        dt (flake/dt f)]
    (if (= const/$id dt)
      (format-reference db spec obj)
      (if (= const/$rdf:json dt)
        (json/parse obj false)
        obj))))

(defn wildcard-spec
  [db cache compact-fn iri]
  (or (get @cache iri)
      (when-let [spec (get-in db [:schema :pred iri])]
        (let [spec* (assoc spec :as (compact-fn (:iri spec)))]
          (vswap! cache assoc iri spec*)
          spec*))))

(defn format-property
  [db cache context compact-fn {:keys [wildcard?] :as select-spec} p-flakes]
  (let [ff  (first p-flakes)
        pid (flake/p ff)
        iri (iri/decode-sid db pid)]
    (when-let [spec (or (get select-spec iri)
                        (when wildcard?
                          (or (wildcard-spec db cache compact-fn iri)
                              (cache-sid->iri db cache compact-fn pid))))]
      (let [p-iri (:as spec)
            v     (if (rdf-type? pid)
                    (type-value db cache compact-fn p-flakes)
                    (let [p-flakes* (if (list-element? ff)
                                      (sort-by (comp :i flake/m) p-flakes)
                                      p-flakes)]
                      (->> p-flakes*
                           (mapv (partial format-object db spec))
                           (util/unwrap-singleton p-iri context))))]
        [p-iri v]))))

(defn format-subject-xf
  [db cache context compact-fn select-spec]
  (comp (partition-by flake/p)
        (map (partial format-property db cache context
                      compact-fn select-spec))
        (remove nil?)))

(defn forward-properties
  [{:keys [t] :as db} iri select-spec context compact-fn cache fuel-tracker error-ch]
  (let [sid                     (iri/encode-iri db iri)
        [start-flake end-flake] (flake-bounds db :spot [sid])
        flake-xf                (when fuel-tracker
                                  (comp (fuel/track fuel-tracker error-ch)))
        range-opts              {:to-t        t
                                 :start-flake start-flake
                                 :end-flake   end-flake
                                 :flake-xf    flake-xf}
        subj-xf                 (comp cat
                                      (format-subject-xf db cache context compact-fn
                                                         select-spec))]
    (->> (query-range/resolve-flake-slices db fuel-tracker :spot error-ch range-opts)
         (async/transduce subj-xf (completing conj) {}))))

(defn reverse-property
  [{:keys [t] :as db} o-iri {:keys [as spec], p-iri :iri, :as reverse-spec} context compact-fn cache fuel-tracker error-ch]
  (let [oid                     (iri/encode-iri db o-iri)
        pid                     (iri/encode-iri db p-iri)
        [start-flake end-flake] (flake-bounds db :opst [oid pid])
        flake-xf                (if fuel-tracker
                                  (comp (fuel/track fuel-tracker error-ch)
                                        (map flake/s))
                                  (map flake/s))
        range-opts              {:to-t        t
                                 :start-flake start-flake
                                 :end-flake   end-flake
                                 :flake-xf    flake-xf}
        sid-xf                  (if spec
                                  (map (partial format-reference db reverse-spec))
                                  (comp (map (partial cache-sid->iri db cache compact-fn))
                                        (map :as)))]
    (->> (query-range/resolve-flake-slices db fuel-tracker :opst error-ch range-opts)
         (async/transduce (comp cat sid-xf)
                          (completing conj
                                      (fn [result]
                                        [as (util/unwrap-singleton as context result)]))
                          []))))

(defn format-subject-flakes
  "current-depth param is the depth of the graph crawl. Each successive 'ref'
  increases the graph depth, up to the requested depth within the select-spec"
  [db cache context compact-fn {:keys [reverse] :as select-spec} current-depth fuel-tracker error-ch s-flakes]
  (if (not-empty s-flakes)
    (let [sid           (->> s-flakes first flake/s)
          s-iri         (iri/decode-sid db sid)
          subject-attrs (into {}
                              (format-subject-xf db cache context compact-fn select-spec)
                              s-flakes)
          subject-ch    (if reverse
                          (let [reverse-ch (subject/format-reverse-properties db s-iri reverse context compact-fn cache fuel-tracker error-ch)]
                            (async/reduce conj subject-attrs reverse-ch))
                          (go subject-attrs))]
      (->> subject-ch
           (subject/resolve-references db cache context compact-fn select-spec current-depth fuel-tracker error-ch)
           (subject/append-id db fuel-tracker s-iri select-spec compact-fn error-ch)))
    (go)))
