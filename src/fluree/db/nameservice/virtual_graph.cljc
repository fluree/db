(ns fluree.db.nameservice.virtual-graph
  (:require [fluree.db.nameservice :as nameservice]
            [fluree.db.util.async :refer [<? go-try]]
            [fluree.db.util.log :as log]))

#?(:clj (set! *warn-on-reflection* true))

(defn publish-virtual-graph
  "Publishes a virtual graph configuration to the nameservice"
  [publisher vg-record]
  (go-try
    (log/debug "Publishing virtual graph:" (:vg-name vg-record))
    (<? (nameservice/publish publisher vg-record))))

(defn retract-virtual-graph
  "Removes a virtual graph from the nameservice"
  [publisher vg-name]
  (go-try
    (<? (nameservice/retract publisher vg-name))))

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