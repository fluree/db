(ns index-explore
  (:require [clojure.java.io :as io]
            [fluree.db.util.json :as json]
            [fluree.db.storage :as storage]))

(def data-directory "./dev/data")

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
  (some (fn [r]
          (when (= t (get r "t"))
            r))
        roots))

(defn latest-t
  [roots]
  (->> roots
       (map #(get % "t"))
       (apply max)))

(defn latest-root
  [roots]
  (at-t (latest-t roots) roots))

(defn idx-branch
  [root idx-type]
  (let [idx*        (name idx-type)
        idx-address (get-in root [idx* "id"])]
    (read-index-file idx-address)))

(defn expand-idx
  [branch]
  (let [children  (get branch "children")
        children* (mapv
                   (fn [child]
                     (let [child-addr (get child "id")
                           child-data (read-index-file child-addr)
                           child*     (merge child child-data)
                           leaf?      (true? (get child "leaf"))]
                       (if leaf?
                         child*
                         (expand-idx child*))))
                   children)]
    (assoc branch "children" children*)))
