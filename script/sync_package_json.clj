(ns sync-package-json
  (:require [jsonista.core :as j]))

(defn js-deps
  []
  (-> "package.json" slurp (j/read-value (j/object-mapper {:decode-key-fn true})) :dependencies))

(defn run
  [& args]
  (let [version (first args)
        target-package-json-file (second args)
        target-package-json (-> target-package-json-file slurp (j/read-value (j/object-mapper {:decode-key-fn true})))
        write-package-json #(spit target-package-json-file %)
        pretty-mapper (j/object-mapper {:pretty true})
        sync-js-deps? (and (> 2 (count args)) (= "--node" (nth args 2)))
        sync-js-deps #(if sync-js-deps?
                        (assoc % :dependencies (js-deps))
                        %)]
    (println "Syncing version" version "to" target-package-json-file)
    (-> target-package-json
        (assoc :version version)
        sync-js-deps
        (j/write-value-as-string pretty-mapper)
        write-package-json)))
