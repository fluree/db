(ns fluree.db.docs
  (:require [codox.main :as codox]
            [clojure.xml :as xml]
            [clojure.java.io :as io]))

(defn name+version-from-pom []
  "Extracts repository name and version from pom.xml file and
  returns as map with :name and :version keys."
  (loop [[x & r] (xml-seq (xml/parse (io/file "pom.xml")))
         groupId    nil
         artifactId nil
         version    nil]
    (let [[groupId* artifactId* version*]
          (cond
            (= :groupId (:tag x)) [(first (:content x)) artifactId version]
            (= :artifactId (:tag x)) [groupId (first (:content x)) version]
            (= :version (:tag x)) [groupId artifactId (first (:content x))]
            :else [groupId artifactId version])]
      (if (or (and groupId* artifactId* version*) (empty? r))
        {:name    (str groupId* "/" artifactId*)
         :version version*}
        (recur r groupId* artifactId* version*)))))

(defn generate [opts]
  "Generates codox docs."
  (codox/generate-docs opts))

(defn -main [& args]
  (let [opts  {:description "Fluree DB Clojure API Documentation"
               :namespaces  ['fluree.db.api]                ;; include only these namespaces in docs
               :output-path "doc/clj"}                      ;; place docs in this folder
        opts* (merge opts (name+version-from-pom))]
    (generate opts*)))