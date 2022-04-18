(ns sync-version
  (:require [clojure.edn :as edn]
            [cheshire.core :as json]))

(defn run
  [& args]
  (let [deps-edn (-> "deps.edn" slurp edn/read-string)
        version (-> deps-edn :aliases :mvn/version)
        package-json-file (first args)
        package-json (-> package-json-file slurp (json/parse-string true))
        write-package-json #(spit package-json-file %)
        pretty-printer (json/create-pretty-printer
                         json/default-pretty-print-options)]
    (println "Syncing version" version "to" package-json-file)
    (-> package-json
        (assoc :version version)
        (json/generate-string {:pretty pretty-printer})
        write-package-json)))
