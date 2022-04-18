(ns sync-package-json
  (:require [clojure.edn :as edn]
            [cheshire.core :as json]))

(defn js-deps
  []
  (-> "package.json" slurp (json/parse-string true) :dependencies))

(defn run
  [& args]
  (let [deps-edn (-> "deps.edn" slurp edn/read-string)
        version (-> deps-edn :aliases :mvn/version)
        target-package-json-file (first args)
        target-package-json (-> target-package-json-file slurp (json/parse-string true))
        write-package-json #(spit target-package-json-file %)
        pretty-printer (json/create-pretty-printer
                         json/default-pretty-print-options)
        sync-js-deps? (= "--node" (second args))
        sync-js-deps #(if sync-js-deps?
                        (assoc %1 :dependencies (js-deps))
                        %1)]
    (println "Syncing version" version "to" target-package-json-file)
    (-> target-package-json
        (assoc :version version)
        sync-js-deps
        (json/generate-string {:pretty pretty-printer})
        write-package-json)))
