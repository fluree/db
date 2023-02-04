(ns fluree.db.permissions-validate
  (:require [fluree.db.dbproto :as dbproto]
            [fluree.db.util.log :as log :include-macros true]
            [fluree.db.flake :as flake]
            [clojure.core.async :refer [go <!] :as async]
            [fluree.db.util.async :refer [<? go-try channel?]]
            [fluree.db.util.core :as util #?(:clj :refer :cljs :refer-macros) [try* catch*]]
            [fluree.db.util.schema :as schema-util]))

#?(:clj (set! *warn-on-reflection* true))


(defn allow-flake?
  "Returns one of:
  (a) exception if there was an error
  (b) truthy value if flake is allowed
  (c) falsey value if flake not allowed

  Note this should only be called if the db is permissioned, don't call if the
  root user as the results will not come back correctly."
  [{:keys [policy] :as db} flake]
  (go-try
    (let [s         (flake/s flake)
          p         (flake/p flake)
          class-ids (or (get @(:cache policy) s)
                        (let [classes (<? (dbproto/-class-ids db (flake/s flake)))]
                          ;; note, classes will return empty list if none found ()
                          (swap! (:cache policy) assoc s classes)
                          classes))
          fns       (keep #(or (get-in policy [:f/view :class % p :function])
                               (get-in policy [:f/view :class % :default :function])) class-ids)]
      (loop [[[async? f] & r] fns]
        ;; return first truthy response, else false
        (if f
          (let [res (if async?
                      (<? (f db flake))
                      (f db flake))]
            (or res
                (recur r)))
          false)))))


(defn- group-policy-by-default
  "Returns map with :default and :property keys, each having k-v tuples of
  their respective policies."
  [policy-maps]
  (->> policy-maps
       (mapcat identity)
       (group-by (fn [policy-map]
                   (if (= :default (key policy-map))
                     :default
                     :property)))))


(defn- group-property-policies
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
    (let [policies-by-pid (group-property-policies property-policies)]
      (loop [[flake & r] flakes
             acc []]
        (if flake
          (let [p-policies (->> flake flake/p (get policies-by-pid))]
            (cond
              p-policies (let [allow? (loop [[[async? f] & r] p-policies]
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
              :else (recur r acc)))
          acc)))))


(defn filter-subject-flakes
  "Takes multiple flakes for the *same* subject and optimizes evaluation
  for the group. Returns the allowed flakes, or an empty vector if none
  are allowed.

  If specific property policies are not defined, a single evaluation for
  the subject can be done and each flake does not need to be checked."
  [{:keys [policy] :as db} flakes]
  (go-try
    (let [fflake         (first flakes)
          class-ids      (<? (dbproto/-class-ids
                               (dbproto/-rootdb db)
                               (flake/s fflake)))
          class-policies (keep #(get-in policy [:f/view :class %]) class-ids)
          {defaults :default props :property} (group-policy-by-default class-policies)
          ;; default-allow? will be the default for all flakes that don't have a property-specific policy
          default-allow? (loop [[[async? f] & r] (eduction
                                                   (map second) (map :function)
                                                   defaults)]
                           ;; return first truthy response, else false
                           (if f
                             (let [res (if async?
                                         (<? (f db fflake))
                                         (f db fflake))]
                               (or res
                                   (recur r)))
                             ;; always default to false! (deny)
                             false))]
      (cond
        props (<? (evaluate-subject-properties db props default-allow? flakes))
        default-allow? flakes
        :else []))))
