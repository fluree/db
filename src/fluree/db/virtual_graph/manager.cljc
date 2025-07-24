(ns fluree.db.virtual-graph.manager
  (:require [clojure.core.async :as async :refer [go go-try <!]]
            [fluree.db.json-ld.iri :as iri]
            [fluree.db.nameservice :as nameservice]
            [fluree.db.util :as util :refer [try* catch*]]
            [fluree.db.util.async :refer [<?]]
            [fluree.db.util.log :as log]
            [fluree.db.virtual-graph :as vg]))

#?(:clj (set! *warn-on-reflection* true))

(defprotocol VirtualGraphManager
  (start [manager]
    "Starts the virtual graph manager, monitoring nameservice for changes")
  (stop [manager]
    "Stops the virtual graph manager")
  (register-virtual-graph [manager vg-record]
    "Registers a new virtual graph from a nameservice record")
  (unregister-virtual-graph [manager vg-id]
    "Unregisters a virtual graph")
  (get-virtual-graph [manager vg-id]
    "Returns the virtual graph instance for the given ID")
  (list-virtual-graphs [manager]
    "Returns all registered virtual graphs")
  (notify-ledger-update [manager ledger-id commit]
    "Notifies the manager that a ledger has been updated"))

(defn vg-record?
  "Returns true if the nameservice record represents a virtual graph"
  [record]
  (some #{"f:VirtualGraphDatabase"} (get record "@type")))

(defn extract-vg-id
  "Extracts the virtual graph ID from a nameservice record"
  [record]
  (get record "@id"))

(defn extract-dependencies
  "Extracts ledger dependencies from a virtual graph record"
  [record]
  (->> (get record "f:dependencies")
       (map #(get % "@id"))
       set))

(defn extract-ledger-id
  "Extracts the base ledger ID from a virtual graph record"
  [record]
  (get-in record ["f:ledger" "@id"]))

(defn create-vg-manager
  "Creates a new virtual graph manager instance"
  [{:keys [nameservice storage monitor-interval]
    :or {monitor-interval 5000}}]
  (let [state (atom {:running false
                     :virtual-graphs {}
                     :dependencies {}
                     :monitor-chan nil})]
    
    (reify VirtualGraphManager
      (start [this]
        (when (:running @state)
          (throw (ex-info "Virtual graph manager already running" {})))
        
        (let [monitor-chan (async/chan)]
          (swap! state assoc :running true :monitor-chan monitor-chan)
          
          ;; Start monitoring loop
          (go
            (try*
              (while (:running @state)
                (try*
                  ;; Get all nameservice records
                  (let [records (<? (nameservice/all-records nameservice))
                        vg-records (filter vg-record? records)]
                    
                    ;; Process each virtual graph record
                    (doseq [record vg-records]
                      (let [vg-id (extract-vg-id record)]
                        (when-not (get-in @state [:virtual-graphs vg-id])
                          (log/info "Found new virtual graph:" vg-id)
                          (<? (register-virtual-graph this record))))))
                  
                  ;; Wait for next check or stop signal
                  (async/alt!
                    monitor-chan ([_] (log/info "Virtual graph manager stopping"))
                    (async/timeout monitor-interval) :continue)
                  
                  (catch* e
                    (log/error "Error in virtual graph monitor loop:" e))))
              
              (catch* e
                (log/error "Fatal error in virtual graph manager:" e))
              
              (finally
                (swap! state assoc :running false)))))
        
        this)
      
      (stop [this]
        (when-let [monitor-chan (:monitor-chan @state)]
          (async/close! monitor-chan))
        (swap! state assoc :running false :monitor-chan nil)
        this)
      
      (register-virtual-graph [this vg-record]
        (go-try
          (let [vg-id (extract-vg-id vg-record)
                ledger-id (extract-ledger-id vg-record)
                dependencies (extract-dependencies vg-record)]
            
            (log/info "Registering virtual graph:" vg-id 
                      "for ledger:" ledger-id
                      "with dependencies:" dependencies)
            
            ;; TODO: Create actual virtual graph instance based on type
            ;; For now, just store the record
            (swap! state update :virtual-graphs assoc vg-id vg-record)
            
            ;; Update dependency tracking
            (doseq [dep dependencies]
              (swap! state update-in [:dependencies dep] (fnil conj #{}) vg-id))
            
            vg-id)))
      
      (unregister-virtual-graph [this vg-id]
        (when-let [vg-record (get-in @state [:virtual-graphs vg-id])]
          (let [dependencies (extract-dependencies vg-record)]
            ;; Remove from virtual graphs
            (swap! state update :virtual-graphs dissoc vg-id)
            
            ;; Remove from dependency tracking
            (doseq [dep dependencies]
              (swap! state update-in [:dependencies dep] disj vg-id)
              (when (empty? (get-in @state [:dependencies dep]))
                (swap! state update :dependencies dissoc dep))))
          
          vg-id))
      
      (get-virtual-graph [this vg-id]
        (get-in @state [:virtual-graphs vg-id]))
      
      (list-virtual-graphs [this]
        (keys (:virtual-graphs @state)))
      
      (notify-ledger-update [this ledger-id commit]
        (go-try
          (let [affected-vgs (get-in @state [:dependencies ledger-id])]
            (when (seq affected-vgs)
              (log/info "Ledger update for" ledger-id "affects virtual graphs:" affected-vgs)
              
              ;; TODO: Trigger actual virtual graph updates
              ;; For now, just log
              (doseq [vg-id affected-vgs]
                (log/debug "Would update virtual graph:" vg-id "due to ledger:" ledger-id)))))))))