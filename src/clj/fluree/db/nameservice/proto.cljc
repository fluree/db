(ns fluree.db.nameservice.proto
  (:refer-clojure :exclude [-lookup]))


(defprotocol iNameService
  (-lookup [nameservice ledger-address] [nameservice ledger-alias opts] "Performs lookup operation on ledger alias and returns map of latest commit and other metadata")
  (-push [nameservice commit-data] "Pushes new commit to nameservice.")
  (-sync? [nameservice] "Indicates if nameservice updates should be performed synchronously, before commit is finalized. Failure will cause commit to fail")

  (-ledgers [nameservice opts] "Returns a list of ledger aliases registered with this nameservice")

  (-close [nameservice] "Closes all resources for this nameservice")

  (-alias [nameservice ledger-address] "Given a ledger address, returns ledger's default alias name else nil, if not avail")
  (-address [nameservice ledger-alias key] "Returns full nameservice address/iri which will get published in commit. If 'private', return nil."))

(defprotocol Publication
  (-subscribe [nameservice ledger-alias callback] "Creates a subscription to nameservice(s) for ledger events. Will call callback with event data as received.")
  (-unsubscribe [nameservice ledger-alias] "Unsubscribes to nameservice(s) for ledger events"))
