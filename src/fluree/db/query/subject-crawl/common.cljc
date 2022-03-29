(ns fluree.db.query.subject-crawl.common
  (:require #?(:clj  [clojure.core.async :refer [go <!] :as async]
               :cljs [cljs.core.async :refer [go <!] :as async])
            [fluree.db.util.async :refer [<? go-try]]
            [fluree.db.dbproto :as dbproto]
            [fluree.db.flake :as flake]
            [fluree.db.util.core :as util :refer [try* catch*]]
            [fluree.db.util.log :as log]
            [fluree.db.util.schema :as schema-util]
            [fluree.db.permissions-validate :as perm-validate]))

#?(:clj (set! *warn-on-reflection* true))

(defn where-subj-xf
  "Transducing function to extract matching subjects from initial where clause."
  [{:keys [start-test start-flake end-test end-flake xf]}]
  (apply comp (cond-> [(map :flakes)
                       (map (fn [flakes]
                              (flake/subrange flakes
                                              start-test start-flake
                                              end-test end-flake)))]
                      xf
                      (conj xf))))


(defn result-af
  [{:keys [db cache fuel-vol max-fuel select-spec error-ch] :as _opts}]
  (fn [flakes port]
    (go
      (try*
        (some->> (<? (fluree.db.query.fql/flakes->res db cache fuel-vol max-fuel select-spec flakes))
                 not-empty
                 (async/put! port))
        (async/close! port)
        (catch* e (async/put! error-ch e) (async/close! port) nil)))))


(defn subj-perm-filter-fn
  "Returns a specific filtering function which takes all subject flakes and
  returns the flakes allowed, or nil if none are allowed."
  [{:keys [permissions] :as db}]
  (let [pred-permissions?  (contains? permissions :predicate)
        coll-permissions   (:collection permissions)
        filter-cache       (atom {})
        default-deny?      (if (true? (:default coll-permissions))
                             false
                             true)
        filter-predicates? (fn [cid]
                             (if-some [cached (get @filter-cache cid)]
                               cached
                               (let [coll-perm (get coll-permissions cid)
                                     filter?   (cond
                                                 (schema-util/is-schema-cid? cid)
                                                 false

                                                 pred-permissions?
                                                 true

                                                 (nil? coll-perm)
                                                 default-deny?

                                                 (and (contains? coll-perm :all)
                                                      (= 1 (count coll-perm)))
                                                 false

                                                 :else true)]
                                 (swap! filter-cache assoc cid filter?)
                                 filter)))]
    (fn [flakes]
      (go-try
        (let [fflake (first flakes)]
          (if (-> fflake flake/s flake/sid->cid filter-predicates?)
            (<? (perm-validate/allow-flakes? db flakes))
            (when (<? (perm-validate/allow-flake? db fflake))
              flakes)))))))


;; TODO - this could be a transducer on flake groups
(defn filter-subject
  "Filters a set of flakes for a single subject and returns true if
  the subject meets the filter map.

  filter-map is a map where pred-ids are keys and values are a list of filtering functions
  where each flake of pred-id must return a truthy value if the subject is allowed."
  [vars filter-map flakes]
  ;; TODO - fns with multiple vars will have to re-calc vars every time, this could be done once for the entire query
  (loop [[f & r] flakes]
    (if f
      (if-let [filter-fns (get filter-map (flake/p f))]
        (when (every? (fn [func] (func f vars)) filter-fns)
          (recur r))
        (recur r))
      flakes)))