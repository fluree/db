(ns fluree.common.protocols)

(defprotocol Service
  (id [_] "Returns the id of the running instance of the service.")
  (stop [_] "Gracefully shuts down the service."))
