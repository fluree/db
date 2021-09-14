(ns fluree.db.docs
  (:require [codox.main :as codox]
            [fluree.db.meta :as meta]
            [clojure.string :as str]))


(defn project-name-and-version
  "Extracts project name and version from metadata (deps.edn or pom.xml) and
  returns them in a map with :name and :version keys."
  []
  {:name (meta/project-name)
   :version (meta/version)})


(defn generate
  "Generates codox docs."
  [opts]
  (codox/generate-docs opts))

(defn exec [opts]
  (as-> opts $
       (update $ :output-path #(when (str/blank? %) "docs")) ;; output docs to this dir by default
       (merge {:description "Fluree DB Clojure API Documentation"
               :namespaces  ['fluree.db.api]} $)   ;; include only these namespaces in docs
       (merge (project-name-and-version) $)
       (generate $)))

(defn -main [& [output-dir]]
  (exec {:output-path output-dir}))
