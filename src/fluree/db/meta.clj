(ns fluree.db.meta
  (:require [clojure.data.xml :as xml]
            [clojure.java.io :as io]
            [clojure.edn :as edn]
            [clojure.string :as str])
  (:import (java.io PushbackReader Closeable))
  (:gen-class))

(set! *warn-on-reflection* true)


(defn find-pom-xml []
  (let [root-file (io/file "pom.xml")]
    (if (.exists root-file)
      root-file
      ;; If it's not at the project root, assume we're running from a JAR and
      ;; look for it in there.
      (io/resource "META-INF/maven/com.fluree/ledger/pom.xml"))))


(defn el-content
  "Get the contents of XML element named el-name (keyword) from
  clojure.data.xml-parsed xml"
  [xml el-name]
  (when-let [el (->> xml (filter #(= (:tag %) el-name)) first)]
    (:content el)))


(defn pom-project []
  (let [pom-xml (find-pom-xml)
        pom     (-> pom-xml slurp (xml/parse-str :namespace-aware false))
        _       (assert (= :project (:tag pom))
                        (str "pom.xml appears malformed; expected top-level project element; got "
                             (:tag pom) " instead"))]
    (:content pom)))


(defn pom-version []
  (let [project (pom-project)]
    (-> project (el-content :version) first)))


(defn deps-edn []
  (let [deps-edn-file (io/file "deps.edn")]
    (when (.exists deps-edn-file)
      (with-open [^Closeable deps-edn-rdr (-> deps-edn-file io/reader PushbackReader.)]
        (edn/read deps-edn-rdr)))))


(defn deps-version []
  (when-let [deps (deps-edn)]
    (-> deps :aliases :mvn/version)))


(defn deps-name []
  (when-let [deps (deps-edn)]
    (let [aliases (:aliases deps)]
      (str/join "/" [(:mvn/group-id aliases) (:mvn/artifact-id aliases)]))))


(defn pom-name []
  (let [project (pom-project)
        group-id (-> project (el-content :groupId) first)
        artifact-id (-> project (el-content :artifactId) first)]
    (str/join "/" [group-id artifact-id])))


(defn version
  "First try getting the version from the deps.edn :mvn/version alias. If that
  fails, try getting it from pom.xml. deps.edn isn't copied into the JAR file,
  so we need a fallback that works in there too."
  []
  (if-let [dv (deps-version)]
    dv
    (pom-version)))


(defn project-name
  "Get project name from deps.edn if available, falling back to pom.xml if not
  (e.g. when running from a JAR)."
  []
  (if-let [dn (deps-name)]
    dn
    (pom-name)))


(defn -main [cmd & _]
  (case cmd
    "name"    (println (project-name))
    "version" (println (version))))
