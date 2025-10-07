(ns fluree.db.nameservice
  (:refer-clojure :exclude [alias])
  (:require [clojure.core.async :as async :refer [go]]
            [fluree.db.util :refer [try* catch*]]
            [fluree.db.util.async :refer [<? go-try]]
            [fluree.db.util.log :as log]))

#?(:clj (set! *warn-on-reflection* true))

(defprotocol iNameService
  (lookup [nameservice ledger-address]
    "Performs lookup operation on ledger alias and returns map of latest commit
    and other metadata")
  (alias [nameservice ledger-address]
    "Given a ledger address, returns ledger's default alias name else nil, if
    not avail")
  (all-records [nameservice]
    "Returns a channel containing all nameservice records for building in-memory query ledger"))

(defprotocol Publisher
  (publish [publisher commit-map]
    "Publishes new commit.")
  (retract [publisher ledger-alias]
    "Remove the nameservice record for the ledger.")
  (publishing-address [publisher ledger-alias]
    "Returns the value to write into the commit's ns field for this nameservice.
    This may be a full address/IRI (e.g., fluree:ipns://...) or a resolvable
    identifier such as a ledger alias (e.g., ledger:branch), depending on the
    nameservice implementation. The returned value will be used with this same
    nameservice's lookup function. If publishing should be private, return nil."))

(defprotocol Publication
  (subscribe [publication ledger-alias]
    "Creates a subscription to publication for ledger events. Will call
    callback with event data as received.")
  (unsubscribe [publication ledger-alias]
    "Unsubscribes to publication for ledger events")
  (known-addresses [publication ledger-alias]))

(defn publish-to-all
  [commit-map publishers]
  (->> publishers
       (map (fn [ns]
              (go
                (try*
                  (<? (publish ns commit-map))
                  (catch* e
                    (log/warn e "Publisher failed to publish commit")
                    ::publishing-error)))))
       async/merge))

(defn published-ledger?
  [nsv ledger-alias]
  (go-try
    (let [addr (<? (publishing-address nsv ledger-alias))]
      (boolean (<? (lookup nsv addr))))))

;; Virtual Graph Dependency Tracking Functions

(defn virtual-graph-record?
  "Checks if a nameservice record is a virtual graph"
  [record]
  (some #{"f:VirtualGraphDatabase"} (get record "@type" [])))

(defn extract-vg-dependencies
  "Extracts ledger dependencies from a VG record"
  [vg-record]
  (mapv #(get % "@id") (get vg-record "f:dependencies" [])))

(defn check-vg-dependencies
  "Returns set of VG names that depend on the ledger, or empty set if none"
  [publisher ledger-alias]
  (get-in @(:vg-state publisher) [:dependencies ledger-alias] #{}))

(defn register-dependencies
  [publisher json-ld]
  (let [vg-name (get json-ld "f:name")
        dependencies (extract-vg-dependencies json-ld)]
    (log/debug "Registering VG dependencies for" vg-name ":" dependencies)
    (swap! (:vg-state publisher)
           (fn [state]
             (reduce (fn [s dep-ledger]
                       (update-in s [:dependencies dep-ledger]
                                  (fnil conj #{}) vg-name))
                     state
                     dependencies)))))

(defn initialize-vg-dependencies
  "Scans all virtual graph records at startup to build dependency map"
  [publisher]
  (go-try
    (let [all-records (<? (all-records publisher))
          vg-records (filter virtual-graph-record? all-records)]

      (log/debug "Initializing VG dependencies from" (count vg-records) "virtual graph records")

      (doseq [vg-record vg-records]
        (<? (register-dependencies publisher vg-record)))

      (log/debug "VG dependency initialization complete. Dependencies:"
                 (:dependencies @(:vg-state publisher))))))

(defn unregister-vg-dependencies
  "Remove dependencies for a deleted virtual graph."
  [publisher vg-name]
  (log/debug "Unregistering VG dependencies for" vg-name)
  (swap! (:vg-state publisher)
         update :dependencies
         (fn [deps]
           (reduce-kv (fn [m ledger vgs]
                        (let [updated-vgs (disj vgs vg-name)]
                          (if (empty? updated-vgs)
                            (dissoc m ledger)
                            (assoc m ledger updated-vgs))))
                      deps  ;; Start with existing deps, not empty map!
                      deps))))
