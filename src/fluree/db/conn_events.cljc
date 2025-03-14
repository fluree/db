(ns fluree.db.conn-events
  (:require [clojure.core.async :as async]
            [fluree.db.util.core :as util #?(:clj :refer :cljs :refer-macros) [try* catch*]]
            [fluree.db.util.log :as log :include-macros true]))

#?(:clj (set! *warn-on-reflection* true))

(defmulti process-event (fn [_ event-type _ _] event-type))

;; internal, connection-specific events to capture and process

(defmethod process-event :set-ws-id
  [conn _ _ ws-id]
  (log/trace "set websocket id:" ws-id)
  (swap! (:state conn) assoc :socket-id ws-id))

(defmethod process-event :default
  [conn event-type subject event-data]
  ;; any event not explicitly captured above is assumed to be a 'global' event.
  ;; send to any registered callback functions
  (log/trace "process event:" event-type event-data)
  (let [callbacks (get-in @(:state conn) [:listeners subject])]
    (doseq [[k f] callbacks]
      (#?(:clj future :cljs do)
        (try* (f event-type event-data)
              (catch* e
                (log/error e
                           (str "Error calling registered callback: " (pr-str k) " for db: " subject
                                ". Event: " event-type " Data: " (pr-str event-data) "."))))))))

(defn process-events
  "Processes incoming events from the ledger.

  Requests that carry a :req-id in the header will attempt to lookup a
  corresponding return channel and send the message along on it.

  Otherwise looks for database events and updates local state accordingly."
  [conn msg]
  (try*
    (let [_          (log/trace "Process events: " msg)
          [event-type subject event-data error-data] msg
          event-type (keyword event-type)
          {:keys [state]} conn]
      (case event-type
        :response (when-let [res-chan (get-in @state [:pending-req subject])]
                    (log/trace "Found response channel for subject" subject)
                    (swap! state update :pending-req #(dissoc % subject))
                    (cond
                      error-data
                      (let [{:keys [message]} error-data
                            exception (ex-info (or message (pr-str error-data))
                                               (dissoc error-data :message))]
                        (async/put! res-chan exception))

                      event-data
                      (async/put! res-chan event-data)

                      :else
                      (async/close! res-chan)))
        :pong nil

        ;; else
        (process-event conn event-type subject event-data)))
    (catch* e
      (log/error e))))
