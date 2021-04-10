(ns fluree.db.docs
  (:require [codox.main :as codox]
            [fluree.db.meta :as meta]))


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


(defn -main [& [output-dir]]
  (let [opts  {:description "Fluree DB Clojure API Documentation"
               :namespaces  ['fluree.db.api]            ;; include only these namespaces in docs
               :output-path (or output-dir "docs")}     ;; place docs in this folder
        opts* (merge opts (project-name-and-version))]
    (generate opts*)))
