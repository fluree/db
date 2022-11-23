(ns sync-package-json
  (:require [clojure.edn :as edn]
            [cheshire.core :as json]))

(defn js-deps
  []
  (-> "package.json" slurp (json/parse-string true) :dependencies))

(defn run
  [& args]
  (let [version (first args)
        target-package-json-file (second args)
        target-package-json (-> target-package-json-file slurp (json/parse-string true))
        write-package-json #(spit target-package-json-file %)
        pretty-printer (json/create-pretty-printer
                         json/default-pretty-print-options)
        sync-js-deps? (and (> 2 (count args)) (= "--node" (nth args 2)))
        sync-js-deps #(if sync-js-deps?
                        (assoc % :dependencies (js-deps))
                        %)]
    (println "Syncing version" version "to" target-package-json-file)
    (-> target-package-json
        (assoc :version version)
        sync-js-deps
        (json/generate-string {:pretty pretty-printer})
        write-package-json)))
