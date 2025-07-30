(ns fluree.db.nameservice.virtual-graph
  (:require [fluree.db.json-ld.iri :as iri]
            [fluree.db.nameservice :as nameservice]
            [fluree.db.util.async :refer [<? go-try]]
            [fluree.db.util.json :as json]
            [fluree.db.util.log :as log]))

#?(:clj (set! *warn-on-reflection* true))

(defn vg-filename
  "Returns the nameservice filename for a virtual graph"
  [vg-name]
  (str "ns@v1/" vg-name ".json"))

(defn vg-record
  "Generates a virtual graph nameservice record"
  [{:keys [vg-name vg-type config dependencies status]
    :or {status "ready"}}]
  {"@context" {"f" iri/f-ns
               "fidx" "https://ns.flur.ee/index#"}
   "@id" vg-name
   "@type" (cond-> ["f:VirtualGraphDatabase"]
             vg-type (conj vg-type))
   "f:name" vg-name
   "f:status" status
   "f:dependencies" (mapv (fn [dep] {"@id" dep}) dependencies)
   "fidx:config" {"@type" "@json"
                  "@value" config}})

(defn publish-virtual-graph
  "Publishes a virtual graph configuration to the nameservice"
  [publisher vg-config]
  (go-try
    (let [vg-record (vg-record vg-config)
          record-bytes (json/stringify-UTF8 vg-record)
          filename (vg-filename (:vg-name vg-config))]
      (log/debug "Published virtual graph successfully:" (:vg-name vg-config))
      (<? (nameservice/publish publisher
                               {"type" "virtual-graph"
                                "record" vg-record
                                "filename" filename
                                "bytes" record-bytes})))))

(defn retract-virtual-graph
  "Removes a virtual graph from the nameservice"
  [publisher vg-name]
  (go-try
    (let [filename (vg-filename vg-name)]
      (<? (nameservice/retract publisher filename)))))

(defn list-virtual-graphs
  "Lists all virtual graphs from the nameservice"
  [nameservice]
  (go-try
    (->> (<? (nameservice/all-records nameservice))
         (filter #(some #{"f:VirtualGraphDatabase"} (get % "@type"))))))

(defn get-virtual-graph
  "Retrieves a specific virtual graph record from the nameservice"
  [nameservice vg-name]
  (go-try
    (or (->> (<? (nameservice/all-records nameservice))
             (filter #(= (get % "@id") vg-name))
             first)
        :not-found)))

(defn virtual-graph-exists?
  "Checks if a virtual graph exists in the nameservice"
  [nameservice vg-name]
  (go-try
    (let [vg (<? (get-virtual-graph nameservice vg-name))]
      (not= :not-found vg))))