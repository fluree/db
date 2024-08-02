(ns fluree.db.nameservice.s3
  (:require [fluree.db.nameservice.proto :as ns-proto]
            [clojure.core.async :as async :refer [go <!]]
            [clojure.string :as str]
            [fluree.db.method.s3.core :as s3]))

(set! *warn-on-reflection* true)

(defn push
  [s3-client s3-bucket s3-prefix {commit-address "address"
                                  nameservices   "ns"}]
  (go
    (let [my-ns-iri   (->> (map #(get % "id") nameservices)
                           (some #(when (re-matches #"^fluree:s3:.+" %) %)))
          commit-path (s3/address-path s3-bucket s3-prefix commit-address false)
          head-path   (s3/address-path s3-bucket s3-prefix my-ns-iri)]
      (->> (.getBytes ^String commit-path)
           (s3/write-s3-data s3-client s3-bucket s3-prefix head-path)
           :address))))

(defn lookup-alias
  [s3-client s3-bucket s3-prefix ledger-alias]
  (go (s3/s3-address s3-bucket s3-prefix (<! (s3/read-address s3-client s3-bucket s3-prefix ledger-alias)))))

(defrecord S3NameService
  [s3-client s3-bucket s3-prefix sync?]
  ns-proto/Publisher
  (-push [_ commit-data] (push s3-client s3-bucket s3-prefix commit-data))

  ns-proto/iNameService
  (-lookup [_ ledger-alias] (lookup-alias s3-client s3-bucket s3-prefix ledger-alias))
  (-lookup [_ ledger-alias opts] (lookup-alias s3-client s3-bucket s3-prefix ledger-alias))
  (-sync? [_] sync?)
  (-ledgers [nameservice opts] (throw (ex-info "Unsupported S3NameService op: ledgers" {})))
  (-address [_ ledger-alias {:keys [branch] :as _opts}]
    (let [branch (if branch (name branch) "main")]
      (go (s3/s3-address s3-bucket s3-prefix (str ledger-alias "/" branch "/head")))))
  (-alias [_ ledger-address]
    (-> ledger-address (->> (s3/address-path s3-bucket s3-prefix)) (str/split #"/")
        (->> (drop-last 2) (str/join #"/"))))
  (-close [nameservice] true))


(defn initialize
  [s3-client s3-bucket s3-prefix]
  (map->S3NameService {:s3-client s3-client
                       :s3-bucket s3-bucket
                       :s3-prefix s3-prefix
                       :sync?     true}))
