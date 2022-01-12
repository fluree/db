(ns fluree.db.json-ld.reify
  (:require [fluree.json-ld :as json-ld]
            [fluree.db.flake :as flake #?@(:cljs [:refer [Flake]])]
            [fluree.db.constants :as const]
            [fluree.db.json-ld.ledger :as jld-ledger]
            [fluree.db.json-ld.vocab :as vocab]
            [fluree.db.util.log :as log])
  #?(:clj (:import (fluree.db.flake Flake))))

;; generates a db/ledger from persisted data
#?(:clj (set! *warn-on-reflection* true))

(def ^:const max-vocab-sid (flake/max-subject-id const/$_collection))

(defn get-iri-sid
  "Gets the IRI for any existing subject ID."
  [iri db iris]
  (if-let [cached (get @iris iri)]
    cached
    ;; TODO following, if a retract was made there could be 2 matching flakes and want to make sure we take the latest add:true
    (when-let [sid (some-> (flake/match-post (get-in db [:novelty :post]) const/$iri iri)
                           first
                           :s)]
      (vswap! iris assoc iri sid)
      sid)))


(defn get-vocab-flakes
  [flakes]
  (flake/subrange flakes
                  >= (flake/->Flake (flake/max-subject-id const/$_collection) -1 nil nil nil nil)
                  <= (flake/->Flake 0 -1 nil nil nil nil)))


(defn get-refs
  "Finds all refs by looking at context. If a property is defined with @type: @id it is a ref.

  Note: this assumes the properties are not compact IRIS - if that changes, they will have to be
  expanded first.

  This also leverages that assertions have been processed, and thus the 'iris' volatile! map will
  contain mappings of new properties to their respective property-ids/sids... this will not work if
  that step has not happened first."
  [commit db iris]
  (let [context  (get commit "@context")
        ;; context* ends up being a list of context-maps (likely only one of them)
        context* (->> (if (sequential? context) context [context])
                      (filter map?))]
    (reduce
      (fn [acc ctx-node]
        (reduce-kv
          (fn [acc* k v]
            (if (= "@id" (get v "@type"))
              (if-let [pid (jld-ledger/get-iri-sid k db iris)]
                (conj acc* pid)
                acc*)
              acc*))
          acc
          ctx-node))
      #{}
      context*)))


(defn retract-node
  [db node t iris]
  (let [{:keys [id type]} node
        sid              (or (get-iri-sid id db iris)
                             (throw (ex-info (str "Retractions specifies an IRI that does not exist: " id)
                                             {:status 400 :error :db/invalid-commit})))
        type-retractions (when type
                           (mapv (fn [type-item]
                                   (let [type-id (or (get-iri-sid type-item db iris)
                                                     (throw (ex-info (str "Retractions specifies an @type that does not exist: " type-item)
                                                                     {:status 400 :error :db/invalid-commit})))]
                                     (flake/->Flake sid const/$rdf:type type-id t false nil)))
                                 type))]
    (reduce-kv
      (fn [acc k v-map]
        (if (keyword? k)
          acc
          (let [pid (or (get-iri-sid k db iris)
                        (throw (ex-info (str "Retraction on a property that does not exist: " k)
                                        {:status 400 :error :db/invalid-commit})))]
            (conj acc (flake/->Flake sid pid (:value v-map) t false nil)))))
      (or type-retractions [])
      node)))


(defn retract-flakes
  [db retractions t iris]
  (reduce
    (fn [acc node]
      (into acc
            (retract-node db node t iris)))
    []
    retractions))


(defn assert-node
  [db node t iris next-pid next-sid]
  (let [{:keys [id type]} node
        existing-sid    (get-iri-sid id db iris)
        sid             (or existing-sid
                            (jld-ledger/generate-new-sid node iris next-pid next-sid))
        type-assertions (if type
                          (mapcat (fn [type-item]
                                    (let [existing-id (or (get-iri-sid type-item db iris)
                                                          (get jld-ledger/predefined-properties type-item))
                                          type-id     (or existing-id
                                                          (jld-ledger/generate-new-pid type-item iris next-pid))
                                          type-flakes (when-not existing-id
                                                        [(flake/->Flake type-id const/$iri type-item t true nil)
                                                         (flake/->Flake type-id const/$rdf:type const/$rdfs:Class t true nil)])]
                                      (into [(flake/->Flake sid const/$rdf:type type-id t true nil)]
                                            type-flakes)))
                                  type)
                          [])
        base-flakes     (if existing-sid
                          type-assertions
                          (conj type-assertions (flake/->Flake sid const/$iri id t true nil)))]
    (reduce-kv
      (fn [acc k v-map]
        (if (keyword? k)
          acc
          (let [existing-pid (get-iri-sid k db iris)
                pid          (or existing-pid
                                 (jld-ledger/generate-new-pid k iris next-pid))]
            (cond-> (if-let [ref-iri (:id v-map)]
                      (let [existing-sid (get-iri-sid ref-iri db iris)
                            ref-sid      (or existing-sid
                                             (jld-ledger/generate-new-sid v-map iris next-pid next-sid))]
                        (cond-> (conj acc (flake/->Flake sid pid ref-sid t true nil))
                                (nil? existing-sid) (conj (flake/->Flake ref-sid const/$iri ref-iri t true nil))))
                      (conj acc (flake/->Flake sid pid (:value v-map) t true nil)))
                    (nil? existing-pid) (conj (flake/->Flake pid const/$iri k t true nil))))))
      base-flakes
      node)))


(defn assert-flakes
  [db assertions t iris]
  (let [last-pid (volatile! (jld-ledger/last-pid db))
        last-sid (volatile! (jld-ledger/last-sid db))
        next-pid (fn [] (vswap! last-pid inc))
        next-sid (fn [] (vswap! last-sid inc))]
    (reduce
      (fn [acc node]
        (into acc
              (assert-node db node t iris next-pid next-sid)))
      []
      assertions)))


(defn merge-flakes
  [{:keys [novelty stats] :as db} t refs flakes]
  (let [bytes #?(:clj (future (flake/size-bytes flakes))    ;; calculate in separate thread for CLJ
                 :cljs (flake/size-bytes flakes))
        {:keys [spot psot post opst size]} novelty
        flakes*       (sort-by #(.-p ^Flake %) flakes)
        vocab-change? (<= (.-s ^Flake (first flakes*)) max-vocab-sid) ;; flakes are sorted, so lowest sid of all flakes will be first
        db*           (assoc db :t t
                                :novelty {:spot (into spot flakes)
                                          :psot (into psot flakes)
                                          :post (into post flakes)
                                          :opst (->> flakes*
                                                     (partition-by #(.-p ^Flake %))
                                                     (reduce
                                                       (fn [opst* p-flakes]
                                                         (let [pid (.-p ^Flake (first p-flakes))]
                                                           (if (or (refs pid) ;; refs is a set of ref pids processed in this commit
                                                                   (get-in db [:schema :pred pid :ref?]))
                                                             (into opst* p-flakes)
                                                             opst*)))
                                                       opst))
                                          :size (+ size #?(:clj @bytes :cljs bytes))}
                                :stats (-> stats
                                           (update :size + #?(:clj @bytes :cljs bytes)) ;; total db ~size
                                           (update :flakes + (count flakes))))]
    (if vocab-change?
      (let [all-refs     (into (get-in db [:schema :refs]) refs)
            vocab-flakes (get-vocab-flakes (get-in db* [:novelty :spot]))]
        (assoc db* :schema (vocab/vocab-map* t all-refs vocab-flakes)))
      db*)))


(defn merge-commit
  [db commit]
  (let [iris           (volatile! {})
        t              (- (get-in commit ["https://flur.ee/ns/block/t" :value]))
        assert         (get commit "https://flur.ee/ns/block/assert")
        retract        (get commit "https://flur.ee/ns/block/retract")
        retract-flakes (retract-flakes db retract t iris)
        assert-flakes  (assert-flakes db assert t iris)
        refs           (get-refs commit db iris)]
    (merge-flakes db t refs (into assert-flakes retract-flakes))))


(defn load-commit
  [read-fn commit-key]
  (let [commit  (read-fn commit-key)
        commit* (json-ld/expand commit)
        subject (get commit* "https://www.w3.org/2018/credentials#credentialSubject")]
    (when-not subject
      (throw (ex-info (str "Unable to retrieve commit subject data from commit: " commit-key ".")
                      {:status      500
                       :error       :db/invalid-commit
                       :commit-data (str (subs (str commit) 0 500) "...")})))
    subject))


;; TODO - validate commit signatures
;; TODO - support both VC and basic commit reading
;; TODO - Check next-commit t is one less than last-t
(defn trace-commits
  [read-fn starting-commit]
  (loop [next-commit starting-commit
         last-t      nil
         commits     (list)]
    (let [commit       (load-commit read-fn next-commit)
          t            (get-in commit ["https://flur.ee/ns/block/t" :value])
          next-commit* (get-in commit ["https://flur.ee/ns/block/prev" :id])
          commits*     (conj commits commit)]
      (if (= 1 t)
        commits*
        (recur next-commit* t commits*)))))


(defn retrieve-genesis
  [{:keys [config] :as db} db-key]
  (let [read-fn (:read config)
        doc     (-> db-key
                    read-fn
                    json-ld/expand)
        t       (get-in doc ["https://flur.ee/ns/block/t" :value])]
    (if (= 1 t)
      doc

      (do
        (log/info "DB has no index service, retrieving blockchain of:" t "commits.")
        ))
    doc))


(defn load-db
  [{:keys [config] :as db} db-key]
  (let [read-fn (:read config)
        commits (trace-commits read-fn db-key)]
    (reduce
      (fn [db* commit]
        (merge-commit db* commit))
      db commits)))


(comment

  (load-db)

  )