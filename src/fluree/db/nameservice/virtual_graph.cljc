(ns fluree.db.nameservice.virtual-graph
  (:require [clojure.string :as str]
            [fluree.db.flake.commit-data :as commit-data]
            [fluree.db.json-ld.iri :as iri]
            [fluree.db.nameservice :as nameservice]
            [fluree.db.nameservice.storage :as ns-storage]
            [fluree.db.util :as util]
            [fluree.db.util.async :refer [<? go-try]]
            [fluree.db.util.json :as json]
            [fluree.db.util.log :as log]))

#?(:clj (set! *warn-on-reflection* true))

(defn vg-filename
  "Returns the nameservice filename for a virtual graph"
  [ledger-alias vg-alias]
  (str "ns@v1/" ledger-alias "##" vg-alias ".json"))

(defn vg-record
  "Generates a virtual graph nameservice record"
  [{:keys [ledger-alias vg-alias vg-type config dependencies status]
    :or {status "ready"}}]
  (let [vg-id (str ledger-alias "##" vg-alias)]
    {"@context" {"f" iri/f-ns
                 "fidx" "https://ns.flur.ee/index#"}
     "@id" vg-id
     "@type" (cond-> ["f:VirtualGraphDatabase"]
               vg-type (conj vg-type))
     "f:ledger" {"@id" ledger-alias}
     "f:virtualGraph" vg-alias
     "f:status" status
     "f:dependencies" (mapv (fn [dep] {"@id" dep}) dependencies)
     "fidx:config" {"@type" "@json"
                    "@value" config}}))

(defn publish-virtual-graph
  "Publishes a virtual graph configuration to the nameservice"
  [publisher {:keys [ledger-alias] :as vg-config}]
  (go-try
    (let [vg-record (vg-record vg-config)
          record-bytes (json/stringify-UTF8 vg-record)
          filename (vg-filename ledger-alias (:vg-alias vg-config))]
      (<? (nameservice/publish publisher 
                               {"type" "virtual-graph"
                                "record" vg-record
                                "filename" filename
                                "bytes" record-bytes})))))

(defn retract-virtual-graph
  "Removes a virtual graph from the nameservice"
  [publisher ledger-alias vg-alias]
  (go-try
    (let [filename (vg-filename ledger-alias vg-alias)]
      (<? (nameservice/retract publisher filename)))))

(defn parse-vg-id
  "Parses a virtual graph ID into its components.
  Returns {:ledger-alias :vg-alias} or nil if invalid."
  [vg-id]
  (when-let [[_ ledger vg] (re-matches #"^(.+?)##(.+)$" vg-id)]
    {:ledger-alias ledger
     :vg-alias vg}))

(defn list-virtual-graphs
  "Lists all virtual graphs for a given ledger from the nameservice"
  [nameservice ledger-alias]
  (go-try
    (let [all-records (<? (nameservice/all-records nameservice))
          prefix (str ledger-alias "##")]
      (->> all-records
           (filter #(str/starts-with? (get % "@id") prefix))
           (filter #(some #{"f:VirtualGraphDatabase"} (get % "@type")))))))

(defn get-virtual-graph
  "Retrieves a specific virtual graph record from the nameservice"
  [nameservice ledger-alias vg-alias]
  (go-try
    (let [vg-id (str ledger-alias "##" vg-alias)
          all-records (<? (nameservice/all-records nameservice))]
      (or (->> all-records
               (filter #(= (get % "@id") vg-id))
               first)
          :not-found))))

(defn virtual-graph-exists?
  "Checks if a virtual graph exists in the nameservice"
  [nameservice ledger-alias vg-alias]
  (go-try
    (let [vg (<? (get-virtual-graph nameservice ledger-alias vg-alias))]
      (not= :not-found vg))))