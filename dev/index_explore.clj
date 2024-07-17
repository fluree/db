(ns index-explore
  (:require [clojure.java.io :as io]
            [fluree.db.util.json :as json]
            [fluree.db.storage :as storage]))

(def data-directory "./data")

(defn set-data-dir!
  [data-dir]
  (alter-var-root #'data-directory (constantly data-dir)))

(defn read-roots
  [ledger-alias]
  (->> (io/file data-directory ledger-alias "index" "root")
       file-seq
       (filter #(.isFile %))
       (map slurp)
       (map (fn [f]
              (json/parse f false)))))

(defn read-index-file
  [address]
  (let [local-path (storage/parse-local-path address)]
    (-> data-directory
        (io/file local-path)
        slurp
        (json/parse false))))

(defn at-t
  [t roots]
  (filter (fn [r]
            (= t (get r "t")))
          roots))
