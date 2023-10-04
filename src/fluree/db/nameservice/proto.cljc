(ns fluree.db.nameservice.proto)


(defprotocol iNameService
  (-lookup [nameservice ledger-alias] [nameservice ledger-alias opts] "Performs lookup operation on ledger alias and returns map of latest commit and other metadata")
  (-push [nameservice commit-data] "Pushes new commit to nameservice.")
  (-subscribe [nameservice ledger-alias callback] "Creates a subscription to nameservice(s) for ledger events")
  (-unsubscribe [nameservice ledger-alias] "Unsubscribes to nameservice(s) for ledger events")
  (-sync? [nameservice] "Indicates if nameservice updates should be performed synchronously, before commit is finalized. Failure will cause commit to fail")

  (-exists? [nameservice ledger-address] "Returns true if ledger exists (must have had at least one commit), false otherwise")
  (-ledgers [nameservice opts] "Returns a list of ledger aliases registered with this nameservice")

  (-close [nameservice] "Closes all resources for this nameservice")

  (-alias [nameservice ledger-address] "Given a ledger address, returns ledger's default alias name else nil, if not avail")
  (-address [nameservice ledger-alias key] "Returns address/iri for provided ledger alias specific to the connection type"))


