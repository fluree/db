(ns fluree.db.indexer.cuckoo
  "Cuckoo filter implementation for cross-branch index garbage collection.
  
  Uses cuckoo filters to efficiently check if index nodes marked as garbage
  by one branch are still in use by other branches."
  (:require [alphabase.core :as alphabase]
            [clojure.string :as str]
            [fluree.db.storage :as store]
            [fluree.db.util :refer [try* catch*]]
            [fluree.db.util.async :refer [go-try <?]]
            [fluree.db.util.bytes :as bytes]
            [fluree.db.util.json :as json]
            [fluree.db.util.log :as log :include-macros true]))

#?(:clj (set! *warn-on-reflection* true))

;; Configuration constants
(def ^:const bucket-size 4)
(def ^:const max-kicks 500)
(def ^:const default-filter-capacity 100000)  ; 100K segments per filter

(defn extract-hash-part
  "Extract the base32 hash from an address string. Public for testing."
  [address]
  (if (string? address)
    (-> address
        (str/split #"/")
        last
        (str/replace #"\.json$" ""))
    address))

(defn- address->bytes
  "Convert an address to bytes for hashing.
  Expects valid base32-encoded SHA-256 hashes."
  [address]
  (let [hash-part (extract-hash-part address)]
    (try*
      ;; Decode base32 SHA-256 hash to raw bytes
      (alphabase/base32->bytes hash-part)
      (catch* e
        ;; Log error and throw - addresses should always be valid base32 SHA-256 hashes
        (log/error e "Failed to decode base32 address:" address
                   "hash-part:" hash-part)
        (throw (ex-info "Invalid base32 address for cuckoo filter"
                        {:address address
                         :hash-part hash-part
                         :error (str e)}))))))

(defn- compute-hashes
  "Compute fingerprint and bucket indices from an address.
  Decodes base32 once and returns [fingerprint bucket1 bucket2].
  Uses FNV-1a 32-bit hash for cross-platform consistency between CLJ and CLJS."
  [address num-buckets]
  (let [hash-bytes (address->bytes address)
        ;; Extract 16-bit fingerprint from first 2 bytes
        fp (bit-or (bit-shift-left (bit-and (first hash-bytes) 0xFF) 8)
                   (bit-and (second hash-bytes) 0xFF))
        ;; FNV-1a 32-bit hash for primary bucket
        ;; Uses first 8 bytes for hashing
        ;; FNV-1a prime: 16777619, offset basis: 2166136261
        fnv-prime 16777619
        fnv-offset 2166136261
        b1-hash (reduce (fn [hash b]
                          ;; FNV-1a: hash = (hash XOR byte) * prime
                          ;; Keep in 32-bit range using unsigned-bit-shift-right
                          (bit-and 0xFFFFFFFF
                                   (* (bit-xor hash (bit-and b 0xFF))
                                      fnv-prime)))
                        fnv-offset
                        (take 8 hash-bytes))
        b1 (mod b1-hash num-buckets)
        ;; Compute alternate bucket using XOR with fingerprint hash
        ;; Simple XOR ensures deterministic b2 calculation
        b2 (mod (bit-xor b1 (hash fp)) num-buckets)]
    [fp b1 b2]))

(defn- bucket-full?
  "Check if a bucket has no empty slots."
  [bucket]
  (>= (count (remove nil? bucket)) bucket-size))

(defn- add-to-bucket
  "Try to add fingerprint to bucket. Returns updated bucket or nil if full."
  [bucket fingerprint]
  (when-not (bucket-full? bucket)
    (let [empty-idx (first (keep-indexed (fn [idx val]
                                           (when (nil? val) idx))
                                         bucket))]
      (when empty-idx
        (assoc bucket empty-idx fingerprint)))))

(defn- remove-from-bucket
  "Remove fingerprint from bucket if present."
  [bucket fingerprint]
  (if-let [idx (first (keep-indexed (fn [idx val]
                                      (when (= val fingerprint) idx))
                                    bucket))]
    (assoc bucket idx nil)
    bucket))

(defn- bucket-contains?
  "Check if bucket contains the fingerprint."
  [bucket fingerprint]
  (some #(= % fingerprint) bucket))

(defn- pick-random-entry
  "Pick a random entry from a bucket for eviction."
  [bucket]
  (let [entries (remove nil? bucket)]
    (when (seq entries)
      (rand-nth entries))))

(defrecord CuckooFilter [buckets num-buckets fingerprint-bits count])

(defn create-filter
  "Create a new cuckoo filter with 16-bit fingerprints.
  
  Parameters:
  - expected-items: Expected number of items to store"
  [expected-items]
  (let [;; Size for ~95% load factor
        num-buckets (-> expected-items
                        (/ (* bucket-size 0.95))
                        Math/ceil
                        long
                        (max 16))  ; Minimum 16 buckets
        ;; Initialize buckets with empty vectors
        buckets     (vec (repeat num-buckets
                                 (vec (repeat bucket-size nil))))]
    (->CuckooFilter buckets num-buckets 16 0)))  ; Always use 16-bit fingerprints

(defn- relocate-and-add
  "Try to relocate existing items to make room for new fingerprint."
  [{:keys [buckets num-buckets] :as filter} bucket-idx fingerprint]
  (loop [kicks     0
         fp        fingerprint
         idx       bucket-idx
         buckets'  buckets]
    (if (>= kicks max-kicks)
      nil  ; Failed to insert
      (let [bucket     (get buckets' idx)
            victim     (pick-random-entry bucket)]
        (if-not victim
          ;; Found empty slot
          (let [updated-bucket (add-to-bucket bucket fp)]
            (when updated-bucket
              (assoc filter :buckets (assoc buckets' idx updated-bucket)
                     :count (inc (:count filter)))))
          ;; Evict victim and try to relocate it
          (let [updated-bucket (-> bucket
                                   (remove-from-bucket victim)
                                   (add-to-bucket fp))
                buckets''      (assoc buckets' idx updated-bucket)
                alt-idx        (mod (bit-xor idx (hash victim)) num-buckets)]
            (if-let [alt-bucket' (add-to-bucket (get buckets'' alt-idx) victim)]
              ;; Successfully relocated victim
              (assoc filter :buckets (assoc buckets'' alt-idx alt-bucket')
                     :count (inc (:count filter)))
              ;; Continue relocating
              (recur (inc kicks) victim alt-idx buckets''))))))))

(defn- add-item-internal
  "Internal add-item for single filter."
  [{:keys [buckets num-buckets] :as filter} sha256-hash]
  (let [[fp b1 b2] (compute-hashes sha256-hash num-buckets)
        b1-bucket (get buckets b1)
        b2-bucket (get buckets b2)]
    (cond
      ;; Try primary bucket
      (not (bucket-full? b1-bucket))
      (let [updated (add-to-bucket b1-bucket fp)]
        (assoc filter :buckets (assoc buckets b1 updated)
               :count (inc (:count filter))))

      ;; Try alternate bucket
      (not (bucket-full? b2-bucket))
      (let [updated (add-to-bucket b2-bucket fp)]
        (assoc filter :buckets (assoc buckets b2 updated)
               :count (inc (:count filter))))

      ;; Try relocating
      :else
      (relocate-and-add filter b1 fp))))

(defn- contains-hash-internal?
  "Internal contains-hash? for single filter."
  [{:keys [buckets num-buckets]} sha256-hash]
  (let [[fp b1 b2] (compute-hashes sha256-hash num-buckets)]
    (or (bucket-contains? (get buckets b1) fp)
        (bucket-contains? (get buckets b2) fp))))

(defn- remove-item-internal
  "Internal remove-item for single filter."
  [{:keys [buckets num-buckets count] :as filter} sha256-hash]
  (let [[fp b1 b2] (compute-hashes sha256-hash num-buckets)
        b1-bucket (get buckets b1)
        b2-bucket (get buckets b2)]
    (cond
      (bucket-contains? b1-bucket fp)
      (assoc filter :buckets (assoc buckets b1 (remove-from-bucket b1-bucket fp))
             :count (dec count))

      (bucket-contains? b2-bucket fp)
      (assoc filter :buckets (assoc buckets b2 (remove-from-bucket b2-bucket fp))
             :count (dec count))

      :else filter)))

;; Serialization for persistence

(defn- encode-buckets
  "Encode buckets to a compact EDN format."
  [buckets _fingerprint-bits]
  {:buckets buckets
   :format  :edn})

(defn- decode-buckets
  "Decode buckets from persisted format."
  [encoded]
  (:buckets encoded))

;; Chain management functions

(defn serialize-single
  "Serialize a single filter. Public for testing."
  [{:keys [buckets num-buckets fingerprint-bits count]}]
  {:f fingerprint-bits
   :buckets (encode-buckets buckets fingerprint-bits)
   :num-buckets num-buckets
   :count count})

(defn single-filter->chain
  "Convert a single filter to chain format. Public for testing."
  [filter]
  {:version 2
   :t nil  ; Will be set when persisting
   :filters [(serialize-single filter)]})

(defn- deserialize-single
  "Deserialize a single filter."
  [{:keys [buckets num-buckets f count]}]
  (let [decoded-buckets (decode-buckets buckets)]
    (->CuckooFilter decoded-buckets num-buckets f count)))

(defn add-item-chain
  "Add an item to the filter chain, creating new filter if needed.
  Proactively creates new filter when current filter reaches 90% capacity."
  [{:keys [filters] :as filter-chain} sha256-hash]
  (loop [idx 0]
    (if (< idx (count filters))
      (let [current-filter (deserialize-single (nth filters idx))]
        (if-let [updated (add-item-internal current-filter sha256-hash)]
          (let [updated-serialized (serialize-single updated)
                ;; Check if this filter is now at 90% capacity
                load-factor (/ (double (:count updated))
                               (* (:num-buckets updated) bucket-size))
                ;; If last filter and at 90% capacity, proactively add new empty filter
                filters' (if (and (= idx (dec (count filters)))
                                  (>= load-factor 0.9))
                           (conj (assoc filters idx updated-serialized)
                                 (serialize-single (create-filter default-filter-capacity)))
                           (assoc filters idx updated-serialized))]
            (assoc filter-chain :filters filters'))
          (recur (inc idx))))
      ;; All full, add new filter
      (let [new-filter (create-filter default-filter-capacity)
            updated (add-item-internal new-filter sha256-hash)]
        (if updated
          (assoc filter-chain :filters (conj filters (serialize-single updated)))
          filter-chain)))))

(defn contains-hash-chain?
  "Check if hash exists in any filter in the chain."
  [{:keys [filters]} sha256-hash]
  (some #(contains-hash-internal? (deserialize-single %) sha256-hash) filters))

(defn remove-item-chain
  "Remove item from whichever filter contains it, removing empty filters."
  [{:keys [filters] :as filter-chain} sha256-hash]
  (let [updated-filters
        (vec (for [f filters]
               (let [filter (deserialize-single f)]
                 (if (contains-hash-internal? filter sha256-hash)
                   (serialize-single (remove-item-internal filter sha256-hash))
                   f))))
        ;; Remove empty filters (count = 0), but keep at least one
        cleaned-filters (vec (remove #(zero? (:count %)) updated-filters))
        ;; Ensure we always have at least one filter
        final-filters (if (empty? cleaned-filters)
                        [(serialize-single (create-filter default-filter-capacity))]
                        cleaned-filters)]
    (assoc filter-chain :filters final-filters)))

(defn batch-add-chain
  "Add multiple items to the filter chain."
  [filter-chain sha256-hashes]
  (reduce add-item-chain filter-chain sha256-hashes))

(defn batch-remove-chain
  "Remove multiple items from the filter chain."
  [filter-chain sha256-hashes]
  (reduce remove-item-chain filter-chain sha256-hashes))

(defn create-filter-chain
  "Create a new filter chain with an initial empty filter."
  []
  {:version 2
   :t nil
   :filters [(serialize-single (create-filter default-filter-capacity))]})

(defn serialize
  "Serialize filter chain for storage."
  [filter-chain]
  filter-chain)

(defn deserialize
  "Deserialize filter from storage."
  [data]
  (cond
    (= (:version data) 2) data
    (nil? (:version data)) data  ; Support test data without version
    :else (throw (ex-info "Unsupported filter version" {:version (:version data)}))))

;; Storage integration

(defn filter-storage-path
  "Get the storage path for a branch's cuckoo filter.
  Returns path like 'ledger-name/index/cuckoo/branch.json'."
  [ledger-alias branch-name]
  (str ledger-alias "/index/cuckoo/" branch-name ".json"))

(defn write-filter
  "Persist a cuckoo filter to storage using explicit filename."
  [index-catalog ledger-alias branch-name t filter]
  (go-try
    (when (and index-catalog (:storage index-catalog) filter)
      (let [serialized (-> (serialize filter)
                           (assoc :t t))
            json-str   (json/stringify serialized)
            bytes      (bytes/string->UTF8 json-str)
            filename   (filter-storage-path ledger-alias branch-name)
            ;; Get the actual store from catalog (usually the default store)
            storage    (:storage index-catalog)
            store      (if (satisfies? store/ByteStore storage)
                         storage
                         (store/get-content-store storage ::store/default))]
        (<? (store/write-bytes store filename bytes))))))

(defn read-filter
  "Read a cuckoo filter from storage using explicit filename.
  Returns nil if filter doesn't exist."
  [index-catalog ledger-alias branch-name]
  (go-try
    (log/debug "read-filter called for" ledger-alias "/" branch-name)
    (if (and index-catalog
             (:storage index-catalog)
             ledger-alias
             branch-name)
      (let [filename (filter-storage-path ledger-alias branch-name)
            _ (log/debug "Looking for filter at:" filename)
            ;; Get the actual store from catalog (usually the default store)
            storage  (:storage index-catalog)
            store    (if (satisfies? store/ByteStore storage)
                       storage
                       (store/get-content-store storage ::store/default))]
        (try*
          (when-let [bytes (<? (store/read-bytes store filename))]
            (log/debug "Found filter file, size:" (count bytes) "type:" (type bytes))
            (let [json-str (if (string? bytes)
                             bytes  ; Already a string, don't convert
                             (bytes/UTF8->string bytes))
                  data     (json/parse json-str true)]
              (deserialize data)))
          (catch* e
            ;; Filter doesn't exist or error reading it
            (log/debug "Error reading filter:" (ex-message e))
            nil)))
      (log/debug "Skipping filter read - missing requirements"
                 "catalog:" (boolean index-catalog)
                 "storage:" (boolean (:storage index-catalog))
                 "ledger:" ledger-alias
                 "branch:" branch-name))))

;; Cleanup functions

(defn delete-filter
  "Delete a cuckoo filter file for a branch."
  [index-catalog ledger-alias branch-name]
  (go-try
    (when (and index-catalog (:storage index-catalog))
      (let [filename (filter-storage-path ledger-alias branch-name)
            ;; Get the actual store from catalog (usually the default store)
            storage  (:storage index-catalog)
            store    (if (satisfies? store/EraseableStore storage)
                       storage
                       (store/get-content-store storage ::store/default))
            ;; Build a full address for deletion from the default content store
            loc      (store/location (store/get-content-store storage ::store/default))
            address  (store/build-address loc filename)]
        (try*
          (<? (store/delete store address))
          (catch* _e
            ;; Filter might not exist, that's ok
            nil))))))

(defn delete-all-filters
  "Delete all cuckoo filter files for a ledger (all branches).
  Used when dropping a ledger."
  [index-catalog ledger-alias]
  (go-try
    (when (and index-catalog (:storage index-catalog))
      (let [cuckoo-dir (str ledger-alias "/index/cuckoo")
            ;; Get the actual store from catalog (usually the default store)
            storage    (:storage index-catalog)
            store      (if (and (satisfies? store/RecursiveListableStore storage)
                                (satisfies? store/EraseableStore storage))
                         storage
                         (store/get-content-store storage ::store/default))
            loc        (store/location (store/get-content-store storage ::store/default))]
        (try*
          ;; List all filter files in the cuckoo directory
          (when-let [files (<? (store/list-paths-recursive store cuckoo-dir))]
            (doseq [file files]
              (try*
                (let [address (store/build-address loc file)]
                  (<? (store/delete store address)))
                (catch* _e
                  ;; Continue even if one file fails
                  nil))))
          (catch* _e
            ;; Directory might not exist, that's ok
            nil))))))

(defn discover-branches
  "Discover all branches for a ledger by scanning storage."
  [storage ledger-alias]
  (go-try
    ;; List all cuckoo filter files under ledger-alias/index/cuckoo/
    (let [cuckoo-path (str ledger-alias "/index/cuckoo/")
          ;; Get the actual store from catalog if needed
          store (if (satisfies? store/RecursiveListableStore storage)
                  storage
                  (store/get-content-store storage ::store/default))]
      (when-let [files (<? (store/list-paths-recursive store cuckoo-path))]
        (->> files
             (filter #(str/ends-with? % ".json"))
             (map #(-> %
                       (str/replace cuckoo-path "")
                       (str/replace ".json" "")))
             distinct
             vec)))))

(defn load-other-branch-filters
  "Load all branch filters except the current one."
  [index-catalog ledger-alias current-branch]
  (go-try
    (let [storage (:storage index-catalog)
          branches (<? (discover-branches storage ledger-alias))
          other-branches (remove #(= % current-branch) branches)]
      (loop [branch-list other-branches
             filters []]
        (if-let [branch (first branch-list)]
          (let [filter (<? (read-filter index-catalog ledger-alias branch))]
            (recur (rest branch-list)
                   (if filter
                     (conj filters filter)
                     filters)))
          filters)))))

(defn any-branch-uses?
  "Check if any of the other branches use this index segment."
  [other-branch-filters sha256-hash]
  (some #(contains-hash-chain? % sha256-hash) other-branch-filters))

;; Metrics and monitoring

(defn load-factor
  "Calculate the current load factor of the filter."
  [{:keys [count num-buckets]}]
  (/ (double count) (* num-buckets bucket-size)))

(defn false-positive-rate
  "Estimate the false positive rate based on current load."
  [{:keys [fingerprint-bits] :as _filter}]
  ;; Approximation: FPR ≈ (2 * bucket-size) / 2^fingerprint-bits
  ;; Note: Load factor could be used for more accurate estimates in the future
  (/ (* 2.0 bucket-size) (Math/pow 2 fingerprint-bits)))

(defn filter-stats
  "Get statistics about the filter."
  [filter]
  {:count (:count filter)
   :capacity (* (:num-buckets filter) bucket-size)
   :load-factor (load-factor filter)
   :estimated-fpr (false-positive-rate filter)
   :fingerprint-bits (:fingerprint-bits filter)})

(defn get-chain-stats
  "Get comprehensive statistics from a filter chain."
  [filter-chain]
  (when (and (:version filter-chain) (seq (:filters filter-chain)))
    (let [filters (map deserialize-single (:filters filter-chain))
          total-count (reduce + (map :count filters))
          total-capacity (reduce + (map #(* (:num-buckets %) bucket-size) filters))
          filter-stats (map (fn [f]
                              {:count (:count f)
                               :capacity (* (:num-buckets f) bucket-size)
                               :load-factor (/ (double (:count f))
                                               (* (:num-buckets f) bucket-size))})
                            filters)]
      {:total-count total-count
       :total-capacity total-capacity
       :overall-load-factor (if (pos? total-capacity)
                              (/ (double total-count) total-capacity)
                              0)
       :filter-count (count filters)
       :filters filter-stats
       :fingerprint-bits (:fingerprint-bits (first filters))})))

;; Simplified API functions for testing
;; These are thin wrappers that delegate to chain operations

(defn add-item
  "Add item to filter chain. For testing."
  [filter-or-chain sha256-hash]
  (if (:version filter-or-chain)
    (add-item-chain filter-or-chain sha256-hash)
    ;; Support for single filter in tests
    (add-item-internal filter-or-chain sha256-hash)))

(defn contains-hash?
  "Check if hash exists in filter. For testing."
  [filter-or-chain sha256-hash]
  (if (:version filter-or-chain)
    (contains-hash-chain? filter-or-chain sha256-hash)
    ;; Support for single filter in tests
    (contains-hash-internal? filter-or-chain sha256-hash)))

(defn remove-item
  "Remove item from filter chain. For testing."
  [filter-or-chain sha256-hash]
  (if (:version filter-or-chain)
    (remove-item-chain filter-or-chain sha256-hash)
    ;; Support for single filter in tests
    (remove-item-internal filter-or-chain sha256-hash)))

(defn batch-add
  "Add multiple items to filter."
  [filter-or-chain sha256-hashes]
  (reduce add-item filter-or-chain sha256-hashes))