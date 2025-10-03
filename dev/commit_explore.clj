(ns commit-explore
  (:require [clojure.java.io :as io]
            [fluree.db.util.json :as json]))

;; helper functions that allow you to find/view commits sitting on disk for
;; debugging purposes

(defn list-files
  "List all files in a directory"
  [source-dir]
  (->> (file-seq (io/file source-dir))
       (filter #(.isFile %))
       (map #(.getName %))))

(defn parse-file
  [source-dir file-name]
  (-> source-dir
      (io/file file-name)
      slurp
      (json/parse false)))

(defn parsed-files
  "Lazily returns every file in a directory as a parsed json object"
  [source-dir]
  (let [files (list-files source-dir)]
    (map (partial parse-file source-dir) files)))

(defn data-file?
  "Truthy if the parsed file is a data file (@type = f:DB)"
  [commit]
  (= ["f:DB"] (get commit "@type")))

(defn commit-file?
  "Truthy if the parsed file is a commit file (@type = f:Commit)"
  [commit]
  (= ["Commit"] (get commit "type")))

(defn only-data-files
  "Lazily filters all files to only return those that are data/db files."
  [commits]
  (filter data-file? commits))

(defn only-commit-files
  "Lazily filters all files to only return those that are commit files."
  [commits]
  (filter commit-file? commits))

(defn for-t
  "Returns all parsed files to only return those that are from the
  provided 't' value (should be two - one commit and one data file)"
  [t commits]
  (filter #(if (commit-file? %)
             (= t (get-in % ["data" "t"]))
             (= t (get % "f:t")))
          commits))

(comment

  (def source-dir "data/redshift/test/main/commit")
  (list-files source-dir)

  (->> (parsed-files source-dir)
       (only-commit-files)
       first)

  (->> (parsed-files source-dir)
       (for-t 156)
       only-commit-files))
