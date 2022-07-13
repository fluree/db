(ns fluree.db.event-bus
  (:require #?(:clj  [clojure.core.async :as async]
               :cljs [cljs.core.async :as async])
            [fluree.db.util.core :as util]))

#?(:clj (set! *warn-on-reflection* true))

;; handles a pub/sub mechanism for pushing out different events to external query peers or internal listeners
;; all events keyed by network + db
(def sub-state (atom {}))

(defn reset-sub
  []
  (reset! sub-state {}))

(defn publish
  "'subject' currently only supports db-ident and
   must be in the form of [network ledger-id]"
  [event-type dbv data]
  (let [db-subs  (keys (get @sub-state dbv))
        evt-subs (keys (get @sub-state event-type))]
    (doseq [sub db-subs]
      (when-not (async/put! sub [event-type dbv data])
        (swap! sub-state update dbv dissoc sub)))
    (doseq [sub evt-subs]
      (when-not (async/put! sub [event-type dbv data])
        (swap! sub-state update event-type dissoc sub)))))


(defn subscribe-db
  "Subscribes to all events for a specific db-ident"
  [dbv c]
  (swap! sub-state assoc-in [dbv c] (util/current-time-millis))
  nil)


(defn unsubscribe-db
  "Unsubscribes channel from db."
  [dbv c]
  (swap! sub-state update dbv dissoc c)
  nil)


(defn subscribe-event
  "Subscribes to all events of a specified event type"
  [event-type c]
  (swap! sub-state assoc-in [event-type c] (util/current-time-millis))
  nil)


(defn unsubscribe-event
  "Unsubscribes channel from event updates."
  [event-type c]
  (swap! sub-state update event-type dissoc c))
