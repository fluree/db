(ns fluree.db.flake.match
  (:refer-clojure :exclude [load vswap!])
  (:require [clojure.core.async :as async]
            [fluree.db.query.exec.where :as where]
            [fluree.db.constants :as const]
            [fluree.db.flake :as flake]
            [fluree.db.util.core :as util :refer [ vswap!]]
            [fluree.db.json-ld.policy :as policy]
            [fluree.db.query.range :as query-range]
            [fluree.db.util.async :refer [<? go-try]]))

(defn class-ids
  "Returns list of class-ids for given subject-id"
  [db subject-id]
  (go-try
    (let [root (policy/root db)]
      (<? (query-range/index-range root :spot = [subject-id const/$rdf:type]
                                   {:flake-xf (map flake/o)})))))

(defn p-prop
  [schema property predicate]
  (assert (#{:id :iri :subclassOf :parentProps :childProps :datatype}
           property)
          (str "Invalid predicate property: " (pr-str property)))
  (get-in schema [:pred predicate property]))

(defn class-prop
  [{:keys [schema] :as _db} meta-key class]
  (if (= :subclasses meta-key)
    (get @(:subclasses schema) class)
    (p-prop schema meta-key class)))

(defn match-id
  [db fuel-tracker solution s-mch error-ch]
  (let [matched-ch (async/chan 2 (comp cat
                                       (partition-by flake/s)
                                       (map first)
                                       (map (fn [f]
                                              (if (where/unmatched-var? s-mch)
                                                (let [var     (where/get-variable s-mch)
                                                      matched (where/match-subject s-mch db f)]
                                                  (assoc solution var matched))
                                                solution)))))
        s-mch*     (where/assign-matched-component s-mch solution)]
    (if-let [s (where/compute-sid db s-mch*)]
      (-> db
          (where/resolve-flake-range fuel-tracker error-ch [s])
          (async/pipe matched-ch))
      (async/close! matched-ch))
    matched-ch))

(defn match-triple
  [db fuel-tracker solution tuple error-ch]
  (let [matched-ch (async/chan 2 (comp cat
                                       (map (fn [flake]
                                              (where/match-flake solution tuple db flake)))))
        db-alias   (:alias db)
        triple     (where/assign-matched-values tuple solution)]
    (if-let [[s p o] (where/compute-sids db triple)]
      (let [pid (where/get-sid p db)]
        (if-let [props (and pid (where/get-child-properties db pid))]
          (let [prop-ch (-> props (conj pid) async/to-chan!)]
            (async/pipeline-async 2
                                  matched-ch
                                  (fn [prop ch]
                                    (let [p* (where/match-sid p db-alias prop)]
                                      (-> db
                                          (where/resolve-flake-range fuel-tracker error-ch [s p* o])
                                          (async/pipe ch))))
                                  prop-ch))

          (-> db
              (where/resolve-flake-range fuel-tracker error-ch [s p o])
              (async/pipe matched-ch))))
      (async/close! matched-ch))
    matched-ch))

(defn with-distinct-subjects
  "Return a transducer that filters a stream of flakes by removing any flakes with
  subject ids repeated from previously processed flakes."
  []
  (fn [rf]
    (let [seen-sids (volatile! #{})]
      (fn
        ;; Initialization: do nothing but initialize the supplied reducing fn
        ([]
         (rf))

        ;; Iteration: keep track of subject ids seen; only pass flakes with new
        ;; subject ids through to the supplied reducing fn.
        ([result f]
         (let [sid (flake/s f)]
           (if (contains? @seen-sids sid)
             result
             (do (vswap! seen-sids conj sid)
                 (rf result f)))))

        ;; Termination: do nothing but terminate the supplied reducing fn
        ([result]
         (rf result))))))

(defn match-class
  [db fuel-tracker solution triple error-ch]
  (let [matched-ch (async/chan 2 (comp cat
                                       (with-distinct-subjects)
                                       (map (fn [flake]
                                              (where/match-flake solution triple db flake)))))
        db-alias   (:alias db)
        triple     (where/assign-matched-values triple solution)]
    (if-let [[s p o] (where/compute-sids db triple)]
      (let [cls        (where/get-sid o db)
            sub-obj    (dissoc o ::sids ::iri)
            class-objs (into [o]
                             (comp (map (fn [cls]
                                          (where/match-sid sub-obj db-alias cls)))
                                   (remove nil?))
                             (class-prop db :subclasses cls))
            class-ch   (async/to-chan! class-objs)]
        (async/pipeline-async 2
                              matched-ch
                              (fn [class-obj ch]
                                (-> (where/resolve-flake-range db fuel-tracker error-ch [s p class-obj])
                                    (async/pipe ch)))
                              class-ch))
      (async/close! matched-ch))
    matched-ch))
