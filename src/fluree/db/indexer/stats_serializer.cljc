(ns fluree.db.indexer.stats-serializer
  "Serialization/deserialization for statistics sketches (HLL and enums).

  Format: JSON v1 for human-readability and debuggability.
  File is written to index/stats-sketches and referenced from root :stats-sketches pointer.

  ## SID Encoding

  Property SIDs are two-tuples [namespace-code local-name], e.g., [100 \"name\"].
  These are serialized as pr-str for JSON keys: \"[100 \\\"name\\\"]\".

  This encoding is consistent with Fluree's internal SID representation and
  ensures proper round-trip serialization without loss of structure."
  (:require #?(:clj [jsonista.core :as json])
            [fluree.db.indexer.hll :as hll])
  #?(:cljs (:require [cljs.reader])))

;; JSON v1 Format Spec (from QUERY_STATS_AND_HLL.md lines 474-498)
;; {
;;   "v": 1,
;;   "ledgerAlias": "ns@v1/db",
;;   "indexedT": 42,
;;   "hll": { "algo": "hll++", "p": 8, "m": 256, "registerBits": 6 },
;;   "registerEncoding": { "format": "base64", "compression": "none" },
;;   "properties": {
;;     "12345": {
;;       "values": {
;;         "registersB64": "...",
;;         "approxNDV": 6789,
;;         "epoch": 2
;;       },
;;       "subjects": {
;;         "registersB64": "...",
;;         "approxNDV": 3456,
;;         "epoch": 2
;;       }
;;     }
;;   }
;; }

(defn serialize-stats-sketches
  "Serialize property HLL sketches to JSON v1 format.

  Parameters:
  - ledger-alias: String ledger identifier
  - indexed-t: Transaction ID that was indexed
  - property-sketches: Map of {property-sid {:values sketch, :subjects sketch}}
                       where property-sid is a two-tuple [ns-code local-name]

  Returns: JSON string"
  [ledger-alias indexed-t property-sketches]
  (let [properties (into {}
                         (map (fn [[sid {:keys [values subjects]}]]
                                ;; SID is a two-tuple [ns-code local-name]
                                ;; Serialize as JSON array for storage
                                [(pr-str sid)
                                 (cond-> {}
                                   values
                                   (assoc "values"
                                          {"registersB64" (hll/serialize values)
                                           "approxNDV" (hll/cardinality values)
                                           "epoch" 1})
                                   subjects
                                   (assoc "subjects"
                                          {"registersB64" (hll/serialize subjects)
                                           "approxNDV" (hll/cardinality subjects)
                                           "epoch" 1}))]))
                         property-sketches)

        stats-map {"v" 1
                   "ledgerAlias" ledger-alias
                   "indexedT" indexed-t
                   "hll" {"algo" "hll++"
                          "p" 8
                          "m" 256
                          "registerBits" 6}
                   "registerEncoding" {"format" "base64"
                                       "compression" "none"}
                   "properties" properties}]

    #?(:clj (json/write-value-as-string stats-map)
       :cljs (js/JSON.stringify (clj->js stats-map)))))

(defn deserialize-stats-sketches
  "Deserialize property HLL sketches from JSON v1 format.

  Parameters:
  - json-str: JSON string in v1 format

  Returns: Map with:
  - :ledger-alias
  - :indexed-t
  - :property-sketches - Map of {property-sid {:values sketch, :subjects sketch}}"
  [json-str]
  (let [data #?(:clj (json/read-value json-str)
                :cljs (js->clj (js/JSON.parse json-str)))
        version (get data "v")

        _ (when (not= 1 version)
            (throw (ex-info "Unsupported stats sketches version"
                            {:version version
                             :supported 1})))

        ledger-alias (get data "ledgerAlias")
        indexed-t (get data "indexedT")
        properties-data (get data "properties")

        property-sketches
        (into {}
              (map (fn [[sid-str prop-data]]
                     ;; SID is serialized as pr-str of [ns-code local-name]
                     ;; e.g., "[100 \"alice\"]"
                     (let [sid #?(:clj (read-string sid-str)
                                  :cljs (cljs.reader/read-string sid-str))
                           values-data (get prop-data "values")
                           subjects-data (get prop-data "subjects")]
                       [sid
                        (cond-> {}
                          values-data
                          (assoc :values
                                 (hll/deserialize (get values-data "registersB64")))

                          subjects-data
                          (assoc :subjects
                                 (hll/deserialize (get subjects-data "registersB64"))))])))
              properties-data)]

    {:ledger-alias ledger-alias
     :indexed-t indexed-t
     :property-sketches property-sketches}))

(defn merge-property-sketches
  "Merge two property-sketches maps (from deserialize or previous indexing).
  Uses HLL register-wise maximum merge.

  Parameters:
  - old-sketches: Map of {property-sid {:values sketch, :subjects sketch}}
  - new-sketches: Map of {property-sid {:values sketch, :subjects sketch}}

  Returns: Merged map with same structure"
  [old-sketches new-sketches]
  (let [all-sids (into (set (keys old-sketches))
                       (keys new-sketches))]
    (into {}
          (map (fn [sid]
                 (let [old (get old-sketches sid)
                       new (get new-sketches sid)

                       merged-values
                       (cond
                         (and (:values old) (:values new))
                         (hll/merge-sketches (:values old) (:values new))

                         (:values old) (:values old)
                         (:values new) (:values new))

                       merged-subjects
                       (cond
                         (and (:subjects old) (:subjects new))
                         (hll/merge-sketches (:subjects old) (:subjects new))

                         (:subjects old) (:subjects old)
                         (:subjects new) (:subjects new))]

                   [sid (cond-> {}
                          merged-values (assoc :values merged-values)
                          merged-subjects (assoc :subjects merged-subjects))])))
          all-sids)))
