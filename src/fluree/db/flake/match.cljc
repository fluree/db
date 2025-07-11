(ns fluree.db.flake.match
  (:refer-clojure :exclude [load vswap!])
  (:require [clojure.core.async :as async :refer [<! >!]]
            [fluree.db.constants :as const]
            [fluree.db.flake :as flake]
            [fluree.db.json-ld.policy :as policy]
            [fluree.db.query.exec.where :as where]
            [fluree.db.query.range :as query-range]
            [fluree.db.util :as util :refer [vswap!]]
            [fluree.db.util.async :refer [<? go-try]]))

#?(:clj (set! *warn-on-reflection* true))

(defn class-ids
  "Returns list of class-ids for given subject-id"
  [db tracker subject-id]
  (go-try
    (let [root (policy/root db)]
      (<? (query-range/index-range root tracker :spot = [subject-id const/$rdf:type]
                                   {:flake-xf (map flake/o)})))))

(defn subclasses
  [{:keys [schema] :as _db} class]
  (get @(:subclasses schema) class))

(defn match-id
  [db tracker solution s-mch error-ch]
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
    (if-let [s (where/compute-sid s-mch* db)]
      (-> db
          (where/resolve-flake-range tracker error-ch [s])
          (async/pipe matched-ch))
      (async/close! matched-ch))
    matched-ch))

(defn match-flakes-xf
  [db solution triple]
  (comp cat
        (map (fn [flake] (where/match-flake solution triple db flake)))))

(defn var-pattern
  [triple]
  (mapv #(if (where/get-variable %) :? :v) triple))

(defmulti resolve-transitive
  (fn [_db _tracker _solution triple _error-ch]
    (var-pattern triple)))

(defmethod resolve-transitive :default
  [_ _ _ _triple error-ch]
  (async/put! error-ch (ex-info "Unsupported transitive path." {:status 400 :error :db/unsupported-transitive-path}))
  (doto (async/chan) async/close!))

(defn get-match-iri
  [var soln]
  (-> soln (get var) where/get-iri))

(defmethod resolve-transitive [:? :v :v]
  [db tracker solution [s p o] error-ch]
  (let [tag          (where/get-transitive-property p)
        p*           (where/remove-transitivity p)
        s-var        (where/get-variable s)
        get-s-iri    (partial get-match-iri s-var)
        initial-soln {s-var o}
        out-ch       (async/chan)]
    (async/go
      (when (= :zero+ tag) (>! out-ch initial-soln))
      (loop [[soln & to-visit] [initial-soln]
             visited-iris      (if (= :zero+ tag) #{(where/get-iri o)} #{})]
        (if soln
          (let [step-solns (<! (async/into [] (where/match-clause db tracker solution [[s p* (get soln s-var)]] error-ch)))
                remove-visited-xf (remove (comp visited-iris (partial get-match-iri s-var)))
                visited-step      (sequence remove-visited-xf step-solns)]
            (<! (async/onto-chan! out-ch visited-step false))
            (recur (into to-visit visited-step)
                   (into visited-iris (map get-s-iri) step-solns)))
          (async/close! out-ch))))
    out-ch))

(defmethod resolve-transitive [:v :v :?]
  [db tracker solution [s p o] error-ch]
  (let [tag          (where/get-transitive-property p)
        p*           (where/remove-transitivity p)
        o-var        (where/get-variable o)
        get-o-iri    (partial get-match-iri o-var)
        initial-soln {o-var s}
        out-ch       (async/chan)]
    (async/go
      (when (= :zero+ tag) (>! out-ch initial-soln))
      (loop [[soln & to-visit] [initial-soln]
             visited-iris      (if (= :zero+ tag) #{(where/get-iri s)} #{})]
        (if soln
          (let [step-solns (<! (async/into [] (where/match-clause db tracker solution [[(get soln o-var) p* o]] error-ch)))
                remove-visited-xf (remove (comp visited-iris get-o-iri))
                visited-step      (sequence remove-visited-xf step-solns)]
            (<! (async/onto-chan! out-ch visited-step false))
            (recur (into to-visit visited-step)
                   (into visited-iris (map get-o-iri) step-solns)))
          (async/close! out-ch))))
    out-ch))

(defn o-match->s-match
  "Strip extra keys from a match on an o-var so taht it can be compared to a match
  from an s-var."
  [mch]
  (select-keys mch [::where/var ::where/sids ::where/iri]))

(defn add-reflexive-solutions
  [s-var o-var solns]
  (into #{}
        (mapcat (fn [{s s-var o o-var :as soln}]
                  (let [s* (assoc s ::where/var o-var)
                        o* (assoc (o-match->s-match o) ::where/var s-var)]
                    [{s-var s o-var s*}
                     {s-var o* o-var (o-match->s-match o)}
                     soln])))
        solns))

(defn transitive-step
  [s-var o-var solns]
  (let [get-solution-iris (juxt (partial get-match-iri s-var)
                                (partial get-match-iri o-var))
        solution-iris     (into #{} (map get-solution-iris) solns)]
    (reduce (fn [results {o o-var s s-var :as soln}]
              (into results
                    ;; do not include solns we've already found
                    (remove #(solution-iris (get-solution-iris %)))
                    ;; join each result o-match to each other result's s-match and create a new solution
                    (reduce (fn [joins {o-match o-var s-match s-var}]
                              (if (= (where/get-iri o) (where/get-iri s-match))
                                (conj joins (assoc soln s-var s o-var o-match))
                                joins))
                            []
                            solns)))
            []
            solns)))

(defn transitive-steps
  [step0 [s p o]]
  (let [tag   (where/get-transitive-property p)
        s-var (where/get-variable s)
        o-var (where/get-variable o)]
    (loop [steps      0
           step-solns step0
           result     (if (= :zero+ tag)
                        (add-reflexive-solutions s-var o-var step0)
                        step0)]
      (if (seq step-solns)
        (let [next-step-solns (transitive-step s-var o-var result)]
          (recur (inc steps) next-step-solns (into result next-step-solns)))
        (async/to-chan! result)))))

(defmethod resolve-transitive [:? :v :?]
  [db tracker solution [s p o] error-ch]
  (let [step-ch (async/into #{} (where/match-clause db tracker solution [[s (where/remove-transitivity p) o]] error-ch))
        soln-ch (async/chan)]
    (async/pipeline-async 2
                          soln-ch
                          (fn [step ch]
                            (-> (transitive-steps step [s p o])
                                (async/pipe ch)))
                          step-ch)
    soln-ch))

(defn match-triple
  [db tracker solution tuple error-ch]
  (let [out-ch     (async/chan 2)
        db-alias   (:alias db)
        triple     (where/assign-matched-values tuple solution)]
    (if-let [[s p o] (where/compute-sids db triple)]
      (let [pid (where/get-sid p db)]
        (if-let [props (and pid (where/get-child-properties db pid))]
          (let [prop-ch (-> props (conj pid) async/to-chan!)]
            (async/pipeline-async 2
                                  out-ch
                                  (fn [prop ch]
                                    (let [p* (where/match-sid p db-alias prop)]
                                      (-> db
                                          (where/resolve-flake-range tracker error-ch [s p* o])
                                          (async/pipe (async/chan 2 (match-flakes-xf db solution tuple)))
                                          (async/pipe ch))))
                                  prop-ch))

          (if (where/get-transitive-property p)
            (-> (resolve-transitive db tracker solution [s p o] error-ch)
                (async/pipe out-ch))
            (-> db
                (where/resolve-flake-range tracker error-ch [s p o])
                (async/pipe (async/chan 2 (match-flakes-xf db solution tuple)))
                (async/pipe out-ch)))))
      (async/close! out-ch))
    out-ch))

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
  [db tracker solution triple error-ch]
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
                             (subclasses db cls))
            class-ch   (async/to-chan! class-objs)]
        (async/pipeline-async 2
                              matched-ch
                              (fn [class-obj ch]
                                (-> (where/resolve-flake-range db tracker error-ch [s p class-obj])
                                    (async/pipe ch)))
                              class-ch))
      (async/close! matched-ch))
    matched-ch))
