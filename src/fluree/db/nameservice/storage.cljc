(ns fluree.db.nameservice.storage
  (:require [clojure.core.async :as async :refer [go]]
            [clojure.string :as str]
            [fluree.db.constants :as const]
            [fluree.db.json-ld.iri :as iri]
            [fluree.db.nameservice :as nameservice]
            [fluree.db.storage :as storage]
            [fluree.db.util :as util]
            [fluree.db.util.async :refer [<? go-try]]
            [fluree.db.util.json :as json]
            [fluree.db.util.ledger :as util.ledger]
            [fluree.db.util.log :as log]))

(defn local-filename
  "Returns the local filename for a ledger's nameservice record.
   Expects ledger-alias to be in format 'ledger:branch'.
   Returns path like 'ns@v2/ledger-name/branch.json'."
  [ledger-alias]
  (let [[ledger-name branch] (util.ledger/ledger-parts ledger-alias)
        branch (or branch const/default-branch-name)]
    (str const/ns-version "/" ledger-name "/" branch ".json")))

(defn new-ns-record
  "Generates nameservice metadata map for JSON storage using new minimal format.
   Expects ledger-alias to be in format 'ledger:branch'."
  [ledger-alias commit-address t index-address index-t]
  (let [[alias branch] (util.ledger/ledger-parts ledger-alias)
        branch (or branch const/default-branch-name)]
    (cond-> {"@context"     {"f" iri/f-ns}
             "@id"          ledger-alias  ;; Already includes :branch
             "@type"        ["f:Database" "f:PhysicalDatabase"]
             "f:ledger"     {"@id" alias}  ;; Just the ledger name without branch
             "f:branch"     branch
             "f:commit"     {"@id" commit-address}
             "f:t"          t
             "f:status"     "ready"}
      index-address (assoc "f:index" {"@id" index-address
                                      "f:t" index-t}))))

(defn get-t
  [ns-record]
  (get ns-record "f:t" 0))

(defn get-index-t
  [ns-record]
  (get-in ns-record ["f:index" "f:t"] 0))

(defn update-commit-address
  [ns-record commit-address commit-t]
  (if (and commit-address commit-t)
    (let [prev-t (get-t ns-record)]
      (if (< prev-t commit-t)
        (assoc ns-record
               "f:t" commit-t
               "f:commit" {"@id" commit-address})
        ns-record))
    ns-record))

(defn update-index-address
  [ns-record index-address index-t]
  (if index-address
    (let [prev-t (get-index-t ns-record)]
      (if (or (nil? index-t) (< prev-t index-t))
        (let [index-record (cond-> {"@id" index-address}
                             index-t (assoc "f:t" index-t))]
          (assoc ns-record "f:index" index-record))
        ns-record))
    ns-record))

(defn set-indexing-status
  "Adds or updates the indexing metadata in a nameservice record.

  Parameters:
    ns-record - The existing nameservice record map
    target-t - The 't' value being indexed
    started - ISO-8601 timestamp when indexing started
    machine-id - Machine identifier (hostname:pid) performing the indexing
    last-heartbeat - ISO-8601 timestamp of last heartbeat (defaults to started)

  Returns updated nameservice record with f:indexing field."
  ([ns-record target-t started machine-id]
   (set-indexing-status ns-record target-t started machine-id started))
  ([ns-record target-t started machine-id last-heartbeat]
   (assoc ns-record "f:indexing" {"f:target-t" target-t
                                  "f:started" started
                                  "f:machine-id" machine-id
                                  "f:last-heartbeat" last-heartbeat})))

(defn clear-indexing-status
  "Removes the indexing metadata from a nameservice record.

  Called when indexing completes (successfully or with error).

  Returns updated nameservice record without f:indexing field."
  [ns-record]
  (dissoc ns-record "f:indexing"))

(defn get-indexing-status
  "Extracts indexing metadata from a nameservice record.

  Returns a map with :target-t, :started, :machine-id, and :last-heartbeat if indexing is in progress,
  or nil if not currently indexing."
  [ns-record]
  (when-let [indexing (get ns-record "f:indexing")]
    {:target-t       (get indexing "f:target-t")
     :started        (get indexing "f:started")
     :machine-id     (get indexing "f:machine-id")
     :last-heartbeat (get indexing "f:last-heartbeat")}))

(defn indexing-stale?
  "Checks if an in-progress indexing operation is stale (no heartbeat for >5 minutes).

  Parameters:
    indexing-status - Map with :last-heartbeat timestamp (ISO-8601 string)

  Returns true if last heartbeat is older than 5 minutes, false otherwise."
  [indexing-status]
  (when-let [last-heartbeat (:last-heartbeat indexing-status)]
    (let [stale-threshold-ms (* 5 60 1000) ; 5 minutes in milliseconds
          last-hb-time       #?(:clj (java.time.Instant/parse last-heartbeat)
                                :cljs (js/Date. last-heartbeat))
          now                #?(:clj (java.time.Instant/now)
                                :cljs (js/Date.))
          elapsed-ms         #?(:clj (.toMillis (java.time.Duration/between last-hb-time now))
                                :cljs (- (.getTime now) (.getTime last-hb-time)))]
      (> elapsed-ms stale-threshold-ms))))

(defn can-start-indexing?
  "Determines if a new indexing operation can start.

  Returns {:can-start? true} if indexing can start.
  Returns {:can-start? false, :reason :already-indexing, :status <status>}
          if indexing is already in progress and not stale."
  [ns-record]
  (if-let [indexing-status (get-indexing-status ns-record)]
    (if (indexing-stale? indexing-status)
      {:can-start? true, :reason :stale-indexing}
      {:can-start? false, :reason :already-indexing, :status indexing-status})
    {:can-start? true}))

(defn update-ns-record
  [ns-record ledger-alias commit-address commit-t index-address index-t]
  (if (some? ns-record)
    (-> ns-record
        (update-commit-address commit-address commit-t)
        (update-index-address index-address index-t))
    (new-ns-record ledger-alias commit-address commit-t
                   index-address index-t)))

(defrecord StorageNameService [store]
  nameservice/Publisher
  (publish [_ data]
    (let [;; Extract data from compact JSON-LD format (both genesis and regular commits now use this)
          ledger-alias (get data "alias")  ;; Already includes :branch
          filename     (local-filename ledger-alias)]
      (log/debug "nameservice.storage/publish start" {:ledger ledger-alias :filename filename})
      (let [commit-address (get data "address")
            commit-t       (get-in data ["data" "t"])
            index-address  (get-in data ["index" "address"])
            index-t        (get-in data ["index" "data" "t"])
            record-updater (fn [ns-record]
                             (update-ns-record ns-record ledger-alias commit-address commit-t
                                               index-address index-t))
            res            (storage/swap-json store filename record-updater)]
        (log/debug "nameservice.storage/publish enqueued" {:ledger ledger-alias :filename filename})
        res)))

  (retract [_ ledger-alias]
    (let [filename (local-filename ledger-alias)
          address  (-> store
                       storage/location
                       (storage/build-address filename))]
      (storage/delete store address)))

  (publishing-address [_ ledger-alias]
    ;; Just return the alias - lookup will handle branch extraction via local-filename
    (go ledger-alias))

  (index-start [_ ledger-alias target-t machine-id]
    (let [filename       (local-filename ledger-alias)
          started        (util/current-time-iso)
          result-ch      (async/chan 1)
          record-updater (fn [ns-record]
                           (let [check-result (can-start-indexing? ns-record)]
                             (if (:can-start? check-result)
                               (do
                                 (async/put! result-ch {:status :started})
                                 (set-indexing-status ns-record target-t started machine-id))
                               (do
                                 (async/put! result-ch (assoc (:status check-result)
                                                              :status :already-indexing))
                                 ns-record))))]
      (log/debug "index-start for" ledger-alias "at t:" target-t "machine:" machine-id)
      (storage/swap-json store filename record-updater)
      result-ch))

  (index-heartbeat [_ ledger-alias]
    (let [filename       (local-filename ledger-alias)
          now            (util/current-time-iso)
          result-ch      (async/chan 1)
          record-updater (fn [ns-record]
                           (if (get ns-record "f:indexing")
                             (do
                               (async/put! result-ch {:status :updated})
                               (assoc-in ns-record ["f:indexing" "f:last-heartbeat"] now))
                             (do
                               (async/put! result-ch {:status :not-indexing})
                               ns-record)))]
      (log/debug "index-heartbeat for" ledger-alias)
      (storage/swap-json store filename record-updater)
      result-ch))

  (index-finish [_ ledger-alias]
    (let [filename       (local-filename ledger-alias)
          record-updater (fn [ns-record]
                           (clear-indexing-status ns-record))]
      (log/debug "index-finish for" ledger-alias)
      (storage/swap-json store filename record-updater)
      (go {:status :completed})))

  nameservice/iNameService
  (lookup [_ ledger-address]
    (go-try
      ;; ledger-address is just the alias (potentially with :branch)
      (let [filename (local-filename ledger-address)]
        (log/debug "StorageNameService lookup:" {:ledger-address ledger-address
                                                 :filename       filename})
        (when-let [record-bytes (<? (storage/read-bytes store filename))]
          (json/parse record-bytes false)))))

  (alias [_ ledger-address]
    ;; TODO: need to validate that the branch doesn't have a slash?
    (-> (storage/get-local-path ledger-address)
        (str/split #"/")
        (->> (drop-last 2) ; branch-name, head
             (str/join "/"))))

  (all-records [_]
    (go-try
      ;; Use recursive listing to support ledger names with '/' characters
      (if (satisfies? storage/RecursiveListableStore store)
        (if-let [list-paths-result (storage/list-paths-recursive store const/ns-version)]
          (loop [remaining-paths (<? list-paths-result)
                 records         []]
            (if-let [path (first remaining-paths)]
              (let [file-content (<? (storage/read-bytes store path))]
                (if file-content
                  (let [content-str (if (string? file-content)
                                      file-content
                                      #?(:clj (let [^bytes bytes-content file-content]
                                                (String. bytes-content "UTF-8"))
                                         :cljs (js/String.fromCharCode.apply nil file-content)))
                        record      (json/parse content-str false)]
                    (recur (rest remaining-paths) (conj records record)))
                  (recur (rest remaining-paths) records)))
              records))
          [])
        ;; Fallback for stores that don't support ListableStore
        (do
          (log/warn "Storage backend does not support RecursiveListableStore protocol")
          [])))))

(defn start
  [store]
  (->StorageNameService store))
