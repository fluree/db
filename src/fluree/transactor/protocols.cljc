(ns fluree.transactor.protocols
  (:refer-clojure :exclude [resolve]))

(defprotocol Transactor
  (commit [txr tx tx-info] "Takes a transaction and persists it as a commit wrapping data.")
  (resolve [txr commit-address] "Returns the commit that corresponds to the commit-address."))
