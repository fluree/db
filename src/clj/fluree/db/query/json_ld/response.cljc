(ns fluree.db.query.json-ld.response
  (:require [fluree.db.util.async :refer [<? go-try]]
            [clojure.core.async :as async :refer [<! >! go go-loop]]
            [fluree.db.permissions-validate :as validate]
            [fluree.db.util.core :as util :refer [try* catch*]]
            [fluree.db.flake :as flake]
            [fluree.db.fuel :as fuel]
            [fluree.db.constants :as const]
            [fluree.db.query.dataset :as dataset]
            [fluree.db.query.range :as query-range]
            [fluree.db.util.log :as log :include-macros true]
            [fluree.db.util.json :as json]
            [fluree.db.json-ld.iri :as iri]))

;; handles :select response map for JSON-LD based queries

#?(:clj (set! *warn-on-reflection* true))

(defn cache-sid->iri
  [db cache compact-fn sid]
  (or (get @cache sid)
      (when-let [iri (or (some-> db :schema :pred (get sid) :iri compact-fn)
                         (some-> (iri/decode-sid db sid) compact-fn))]
        (vswap! cache assoc sid {:as iri})
        {:as iri})))

(defn wildcard-spec
  [db cache compact-fn iri]
  (or (get @cache iri)
      (when-let [spec (get-in db [:schema :pred iri])]
        (let [spec* (assoc spec :as (compact-fn (:iri spec)))]
          (vswap! cache assoc iri spec*)
          spec*))))

(defn rdf-type?
  [pid]
  (= const/$rdf:type pid))

(declare format-node)

(defn includes-id?
  [db sid {:keys [wildcard?] :as select-spec}]
  (go-try
    (when (or wildcard?
              (contains? select-spec const/iri-id))
      (<? (validate/allow-iri? db sid)))))

(defn list-element?
  [flake]
  (-> flake flake/m (contains? :i)))

(defn unwrap-singleton
  ([coll]
   (if (= 1 (count coll))
     (first coll)
     coll))

  ([iri context coll]
   (if (#{:list :set} (-> context (get iri) :container))
     coll
     (unwrap-singleton coll))))

(defn type-value
  [db cache compact-fn type-flakes]
  (->> type-flakes
       (into [] (comp (map flake/o)
                      (map (partial cache-sid->iri db cache compact-fn))
                      (map :as)))
       unwrap-singleton))

(defn format-reference
  [spec sid]
  {::reference {:sid  sid
                :spec spec}})

(defn format-object
  [spec f]
  (let [obj (flake/o f)
        dt (flake/dt f)]
    (if (= const/$xsd:anyURI dt)
      (format-reference spec obj)
      (let [obj (flake/o f)]
        (if (= const/$rdf:json dt)
          (json/parse obj false)
          obj)))))

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
                           (mapv (partial format-object spec))
                           (unwrap-singleton p-iri context))))]
        [p-iri v]))))

(defn format-subject-xf
  [db cache context compact-fn select-spec]
  (comp (partition-by flake/p)
        (map (partial format-property db cache context
                      compact-fn select-spec))
        (remove nil?)))

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

(defn reference?
  [v]
  (some? (::reference v)))

(defn display-reference
  [db spec select-spec cache context compact-fn current-depth fuel-tracker error-ch oid]
  (go
    (try*
      (let [;; TODO - we generate id-key here every time, this should be done in the :spec once beforehand and used from there
            max-depth     (:depth select-spec)
            id-key        (:as (or (wildcard-spec db cache compact-fn const/$id)
                                   (cache-sid->iri db cache compact-fn const/$id)))
            o-iri         (iri/decode-sid db oid)
            o-iri-compact (compact-fn o-iri)
            subselect     (:spec spec)
            ref           (cond
                            ;; have a specified sub-selection (graph crawl)
                            subselect
                            (let [ref-attrs (<! (format-node db o-iri context compact-fn subselect cache (inc current-depth) fuel-tracker error-ch))]
                              (if (<? (includes-id? db oid subselect))
                                (assoc ref-attrs id-key o-iri-compact)
                                ref-attrs))

                            ;; requested graph crawl depth has not yet been reached
                            (< current-depth max-depth)
                            (cond-> (<! (format-node db o-iri context compact-fn select-spec cache (inc current-depth) fuel-tracker error-ch))
                              (<? (validate/allow-iri? db oid)) (assoc id-key o-iri-compact))

                            :else
                            (when (<? (validate/allow-iri? db oid))
                              {id-key o-iri-compact}))]
        (not-empty ref))
      (catch* e
              (log/error e "Error resolving reference")
              (>! error-ch e)))))

(defn resolve-reference
  [db cache context compact-fn select-spec current-depth fuel-tracker error-ch v]
  (let [{:keys [sid spec]} (::reference v)]
    (display-reference db spec select-spec cache context
                       compact-fn current-depth fuel-tracker error-ch sid)))

(defn resolve-references
  [db cache context compact-fn select-spec current-depth fuel-tracker error-ch attr-ch]
  (go (when-let [attrs (<! attr-ch)]
        (loop [[[prop v] & r] attrs
               resolved-attrs {}]
          (if prop
            (let [v' (if (sequential? v)
                       (loop [[value & r]     v
                              resolved-values []]
                         (if value
                           (if (reference? value)
                             (if-let [resolved (<! (resolve-reference db cache context compact-fn select-spec current-depth fuel-tracker error-ch value))]
                               (recur r (conj resolved-values resolved))
                               (recur r resolved-values))
                             (recur r (conj resolved-values value)))
                           (not-empty resolved-values)))
                       (if (reference? v)
                         (<! (resolve-reference db cache context compact-fn select-spec current-depth fuel-tracker error-ch v))
                         v))]
              (if (some? v')
                (recur r (assoc resolved-attrs prop v'))
                (recur r resolved-attrs)))
            resolved-attrs)))))

(defn reverse-property
  [{:keys [conn t] :as db} cache compact-fn oid {:keys [as spec], p-iri :iri, :as reverse-spec} fuel-tracker error-ch]
  (let [pid                     (iri/encode-iri db p-iri)
        opst-root               (:opst db)
        opst-novelty            (get-in db [:novelty :opst])
        [start-flake end-flake] (flake-bounds db :opst [oid pid])
        flake-xf                (if fuel-tracker
                                  (comp (fuel/track fuel-tracker error-ch)
                                        (map flake/s))
                                  (map flake/s))
        range-opts              {:from-t      t
                                 :to-t        t
                                 :start-flake start-flake
                                 :end-flake   end-flake
                                 :flake-xf    flake-xf}
        range-ch                (query-range/resolve-flake-slices conn opst-root opst-novelty
                                                                  error-ch range-opts)
        sid-xf                  (if spec
                                  (map (partial format-reference reverse-spec))
                                  (comp (map (partial cache-sid->iri db cache compact-fn))
                                        (map :as)))]

    (async/transduce (comp cat sid-xf)
                     (completing conj
                                 (fn [result]
                                   [as (unwrap-singleton result)]))
                     []
                     range-ch)))

(defn reverse-properties
  [db iri cache compact-fn reverse-map fuel-tracker error-ch]
  (let [out-ch (async/chan 32)
        oid    (iri/encode-iri db iri)]
    (async/pipeline-async 32
                          out-ch
                          (fn [reverse-spec ch]
                            (-> db
                                (reverse-property cache compact-fn oid reverse-spec fuel-tracker error-ch)
                                (async/pipe ch)))
                          (async/to-chan! (vals reverse-map)))

    (async/reduce conj {} out-ch)))

(defn append-id
  [db sid select-spec compact-fn error-ch subgraph-ch]
  (go
    (try*
      (let [subgraph (<! subgraph-ch)
            node     (if (<? (includes-id? db sid select-spec))
                       (let [iri (compact-fn (iri/decode-sid db sid))]
                         (assoc subgraph (compact-fn const/iri-id) iri))
                       subgraph)]
        (not-empty node))
      (catch* e
              (log/info e "Error appending subject iri")
              (>! error-ch e)))))

(defn format-forward-properties
  [{:keys [conn t] :as db} iri context compact-fn select-spec cache fuel-tracker error-ch]
  (let [sid                     (iri/encode-iri db iri)
        spot-root               (:spot db)
        spot-novelty            (-> db :novelty :spot)
        [start-flake end-flake] (flake-bounds db :spot [sid])
        flake-xf                (when fuel-tracker
                                  (comp (fuel/track fuel-tracker error-ch)))
        range-opts              {:from-t      t
                                 :to-t        t
                                 :start-flake start-flake
                                 :end-flake   end-flake
                                 :flake-xf    flake-xf}
        flake-slice-ch          (->> (query-range/resolve-flake-slices conn spot-root spot-novelty
                                                                       error-ch range-opts)
                                     (query-range/filter-authorized db start-flake end-flake error-ch))
        subj-xf                 (comp cat
                                      (format-subject-xf db cache context compact-fn
                                                         select-spec))]
    (async/transduce subj-xf (completing conj) {} flake-slice-ch)))

(defn format-node
  [db iri context compact-fn {:keys [reverse] :as select-spec} cache current-depth fuel-tracker error-ch]
  (let [sid        (iri/encode-iri db iri)
        forward-ch (format-forward-properties db iri context compact-fn select-spec cache fuel-tracker error-ch)
        subject-ch (if reverse
                     (let [reverse-ch (reverse-properties db iri cache compact-fn reverse fuel-tracker error-ch)]
                       (->> [forward-ch reverse-ch]
                            async/merge
                            (async/reduce merge {})))
                     forward-ch)]
    (->> subject-ch
         (resolve-references db cache context compact-fn select-spec current-depth fuel-tracker error-ch)
         (append-id db sid select-spec compact-fn error-ch))))

(defn format-subject-flakes
  "depth-i param is the depth of the graph crawl. Each successive 'ref' increases the graph depth, up to
  the requested depth within the select-spec"
  [db cache context compact-fn {:keys [reverse] :as select-spec} current-depth fuel-tracker error-ch s-flakes]
  (if (not-empty s-flakes)
    (let [sid           (->> s-flakes first flake/s)
          s-iri         (iri/decode-sid db sid)
          subject-attrs (into {}
                              (format-subject-xf db cache context compact-fn select-spec)
                              s-flakes)
          subject-ch    (if reverse
                          (let [reverse-ch (reverse-properties db s-iri cache compact-fn reverse fuel-tracker error-ch)]
                            (async/reduce conj subject-attrs reverse-ch))
                          (go subject-attrs))]
      (->> subject-ch
           (resolve-references db cache context compact-fn select-spec current-depth fuel-tracker error-ch)
           (append-id db sid select-spec compact-fn error-ch)))
    (go)))
