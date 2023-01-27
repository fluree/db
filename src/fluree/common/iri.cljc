(ns fluree.common.iri
  (:refer-clojure :exclude [set reverse list type import]))

(def fluree "https://ns.flur.ee/")

(defn iri
  [component property]
  (str fluree component "#" property))

(def base "@base")
(def context "@context")
(def graph "@graph")
(def id "@id")
(def list "@list")
(def reverse "@reverse")
(def type "@type")
(def value "@value")
(def vocab "@vocab")

(def container "@container")
(def direction "@direction")
(def import "@import")
(def included "@included")
(def index "@index")
(def json "@json")
(def language "@language")
(def nest "@nest")
(def none "@none")
(def prefix "@prefix")
(def propagate "@propagate")
(def protected "@protected")
(def set "@set")
(def version "@version")


(def keywords #{base context graph id list reverse type value vocab container direction import included index
                json language nest none prefix propagate protected set version})

;; Tx
(def Tx (str fluree "Tx/"))
(def TxData (iri "Tx" "data"))
(def TxAuthClaims (iri "Tx" "authClaims"))

;; Commit
(def Commit (str fluree "Commit/"))
(def CommitTx (iri "Commit" "tx"))
(def CommitSize (iri "Commit" "size"))
(def CommitT (iri "Commit" "t"))
(def CommitV (iri "Commit" "v"))
(def CommitPrevious (iri "Commit" "previous"))

;; CommitSummary
(def CommitSummary (str fluree "CommitSummary/"))
(def CommitAddress (iri "Commit" "address"))

;; TxSummary
(def TxSummary (str fluree "TxSummary/"))
(def TxSummaryTx (iri "TxSummary" "tx"))
(def TxSummaryTxAddress (iri "TxSummary" "txAddress"))
(def TxSummaryTxId (iri "TxSummary" "txId"))
(def TxSummarySize (iri "TxSummary" "size"))
(def TxSummaryT (iri "TxSummary" "t"))
(def TxSummaryV (iri "TxSummary" "v"))
(def TxSummaryPrevious (iri "TxSummary" "previous"))

;; TxHead
(def TxHead (str fluree "TxHead/"))
(def TxHeadAddress (iri "TxHead" "address"))

;; Query
(def Query (str fluree "Query/"))
(def QueryQuery (iri "Query" "query"))
(def QueryAuthClaims (iri "Query" "authClaims"))

;; DbBlock
(def DbBlock (str fluree "DbBlock/"))
(def DbBlockAssert (iri "DbBlock" "assert"))
(def DbBlockRetract (iri "DbBlock" "retract"))
(def DbBlockSize (iri "DbBlock" "size"))
(def DbBlockT (iri "DbBlock" "t"))
(def DbBlockV (iri "DbBlock" "v"))
(def DbBlockTxId (iri "DbBlock" "txId"))
(def DbBlockReindexMin (iri "DbBlock" "reindexMin"))
(def DbBlockReindexMax (iri "DbBlock" "reindexMax"))
(def DbBlockIndexRoot (iri "DbBlock" "indexRoot"))
(def DbBlockPrevious (iri "DbBlock" "previous"))

;; DbBlockSummary
(def DbBlockSummary (str fluree "DbBlockSummary/"))
(def DbBlockAddress (iri "DbBlock" "address"))

;; Ledger
(def Ledger (str fluree "Ledger/"))
(def LedgerName (iri "Ledger" "name"))
(def LedgerV (iri "Ledger" "v"))
(def LedgerAddress (iri "Ledger" "address"))
(def LedgerHead (iri "Ledger" "head"))
(def LedgerContext (iri "Ledger" "context"))

(def LedgerEntry (str fluree "LedgerEntry/"))
(def LedgerEntryCreated (iri "LedgerEntry" "created"))
(def LedgerEntryPrevious (iri "LedgerEntry" "previous"))
(def LedgerEntryCommit (iri "LedgerEntry" "commit"))
(def LedgerTxHead (iri "LedgerEntry" "txHead"))
(def LedgerEntryDb (iri "LedgerEntry" "db"))
