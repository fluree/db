(ns fluree.db.permissions-validate
  (:require [fluree.db.constants :as const]
            [fluree.db.dbproto :as dbproto]
            [fluree.db.util.log :as log :include-macros true]
            [fluree.db.flake :as flake]
            [fluree.db.json-ld.iri :as iri]
            [fluree.db.policy.enforce :as enforce]
            [fluree.db.util.async :refer [<? go-try]]))

#?(:clj (set! *warn-on-reflection* true))

(defn unrestricted-view?
  [{:keys [policy] :as _db}]
  (true? (get-in policy [const/iri-view :root?])))

(defn unrestricted-modify?
  [{:keys [policy] :as _db}]
  (true? (get-in policy [const/iri-modify :root?])))

(defn class-restrictions?
  [policy]
  (get-in policy [const/iri-view :class]))

(defn property-restrictions?
  [policy]
  (get-in policy [const/iri-view :property]))

(defn allow-flake?
  "Returns one of:
  (a) exception if there was an error
  (b) truthy value if flake is allowed
  (c) falsey value if flake not allowed"
  [{:keys [policy] :as db} flake]
  (go-try
   (cond

     (enforce/unrestricted? policy false)
     true

     ;; currently property-restrictions override class restrictions if present
     (property-restrictions? policy)
     (<? (enforce/property-allow? db false flake))

     (class-restrictions? policy)
     (let [sid       (flake/s flake)]
       (<? (enforce/class-allow? db sid false nil)))

     :else ;; no restrictions, use default
     (:default-allow? policy))))

(defn allow-iri?
  [db sid]
  (let [id-flake (flake/create sid const/$id nil nil nil nil nil)]
    (allow-flake? db id-flake)))

(defn group-property-policies
  "Returns a map of property policies grouped by property-id (pid).
  For each pid key, returns a vector containing only the function tuples (not entire policy map) of
  [async? fn]. Note while many will have only one fn per pid, there can be multiple - and the first
  truthy response allows access."
  [property-policies]
  (reduce (fn [acc [pid policy-m]]
            (assoc acc pid (conj (get acc pid []) (:function policy-m))))
          {} property-policies))


(defn- evaluate-subject-properties
  [db property-policies default-allow? flakes]
  (go-try
    (let [policies-by-property (group-property-policies property-policies)]
      (loop [[flake & r] flakes
             acc         []]
        (if flake
          (let [prop          (iri/decode-sid db (flake/p flake))
                prop-policies (get policies-by-property prop)]
            (cond
              prop-policies  (let [allow? (loop [[[async? f] & r] prop-policies]
                                            ;; return first truthy response, else false
                                            (if f
                                              (let [res (if async?
                                                          (<? (f db flake))
                                                          (f db flake))]
                                                (or res
                                                    (recur r)))
                                              ;; always default to false! (deny)
                                              false))]
                               (if allow?
                                 (recur r (conj acc flake))
                                 (recur r acc)))
              default-allow? (recur r (conj acc flake))
              :else          (recur r acc)))
          acc)))))

(defn group-policies-by-default
  "Groups policies for the specified action (e.g. :f/view, :f/modify)
  and provided class subject ids by either :default or :property.

  Returns map with :default and :property keys, each having k-v tuples of
  their respective policies"
  [policy action class-iris]
  (->> class-iris
       (keep #(get-in policy [action :class %]))
       (mapcat identity)
       (group-by (fn [policy-map]
                   (if (= :default (key policy-map))
                     :default
                     :property)))))

(defn allow-by-default?
  "Returns true or false if the default policy allows, or denies access
  to the subject's flakes.

  default-allow-policies is as output by group-policies-by-default which will
  be a two-tuple where the second position is the default policy map."
  [db flake default-allow-policies]
  (go-try
    (loop [[[async? f] & r] (eduction
                             (map second) (map :function)
                             default-allow-policies)]
      ;; return first truthy response, else false
      (if f
        (let [f-res (if async?
                      (<? (f db flake))
                      (f db flake))]
          (if f-res
            true
            (recur r)))
        ;; always default to false! (deny)
        false))))


(defn filter-subject-flakes
  "Takes multiple flakes for the *same* subject and optimizes evaluation
  for the group. Returns the allowed flakes, or an empty vector if none
  are allowed.

  Supply action evaluation is for, e.g. :f/view or :f/modify

  If no property policies are not defined, a single evaluation for
  the subject can be done and each flake does not need to be checked."
  [{:keys [policy] :as db} flakes]
  (go-try
    (when-let [fflake (first flakes)]
      (let [class-ids  (<? (dbproto/-class-ids db (flake/s fflake)))
            class-iris (map (partial iri/decode-sid db)
                            class-ids)
            {defaults :default props :property} (group-policies-by-default
                                                 policy const/iri-view class-iris)
            ;; default-allow? will be the default for all flakes that don't have a property-specific policy
            default-allow? (<? (allow-by-default? db fflake defaults))]
        (cond
          props (<? (evaluate-subject-properties db props default-allow? flakes))
          default-allow? flakes
          :else [])))))
